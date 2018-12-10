package redisc

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"golang.org/x/sync/semaphore"
)

// waitPool wrap the redis pool, make it protected by semaphore to limit the access
type waitPool struct {
	*redis.Pool
	sem      *semaphore.Weighted
	waitTime time.Duration
}

func newWaitPool(pool *redis.Pool, maxActive, waitTimeMs int) *waitPool {
	if maxActive == 0 { // TODO : do it in upper layer
		maxActive = 20
	}
	if waitTimeMs == 0 { // TODO : do it in upper layer
		waitTimeMs = 1000
	}

	return &waitPool{
		Pool:     pool,
		sem:      semaphore.NewWeighted(int64(maxActive)),
		waitTime: time.Duration(waitTimeMs) * time.Millisecond,
	}
}

// getWait get connection from the pool.
// If pool is full (no available connection) it wait for the configured timeout,
// before returning invalid conn.
// (default behaviour is to simply return invalid conn when pool is full)
func (wp *waitPool) getWait() (redis.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), wp.waitTime)
	defer cancel()

	err := wp.sem.Acquire(ctx, 1)
	if err != nil {
		return nil, err
	}

	conn := wp.Pool.Get()

	return &connSem{
		Conn: conn,
		sem:  wp.sem,
	}, nil
}

// connSem is redis connection protected by semaphore
type connSem struct {
	sem *semaphore.Weighted
	redis.Conn
}

func (c *connSem) Close() error {
	c.Conn.Close()
	c.sem.Release(1)
	return nil
}
