package redis_stream

import (
	"github.com/garyburd/redigo/redis"
	"github.com/pkg/errors"
)

// RedisWriter publishes messages to the Redis CHANNEL
type RedisSender struct {
	pool     *redis.Pool
	messages chan []byte
}

// NewRedisWriter creates a RedisSender that will use the provided
// redis.Pool.
// 구조체 생성자 패턴
func NewRedisSender(pool *redis.Pool) *RedisSender {
	return &RedisSender{
		pool:     pool,
		messages: make(chan []byte, 10000),
	}
}

// Run the main RedisSender loop that publishes incoming messages to Redis.
func (rs *RedisSender) Run(redisChannelName string) error {
	conn := rs.pool.Get()
	defer conn.Close()

	// range를 사용하여 채널에서 값을 꺼냄
	// for 반복문 안에서 range 키워드를 사용하면 채널이 닫힐 때까지 반복하면서 값을 꺼낸다.
	// range 특징
	// 채널을 닫으면 range 루프가 종료된다.
	// 채널이 열려있고, 값이 들어오지 않는다면 range는 실행되지 않고 계속 대기한다.
	// 만약 다른 곳에서 채널에 값을 보냈다면(채널에 값이 들어오면) 그때부터 range가 계속 반복된다.
	for data := range rs.messages {
		if err := publishToRedis(conn, data, redisChannelName); err != nil {
			rs.Publish(data) // attempt to redeliver later
			return err
		}
	}

	// 위 소스는 다음과 같다.
	// for {
	// 	data, isChannelOpened := <-rs.messages
	// 	if isChannelOpened == true {
	// 		if err := publishToRedis(conn, data, redisChannelName); err != nil {
	// 			rs.Publish(data) // attempt to redeliver later
	// 			return err
	// 		}
	// 	} else {
	// 		beego.Warn(redisChannelName + " is closed")
	// 		break
	// 	}
	// }

	return nil
}

// publish to Redis via channel.
func (rs *RedisSender) Publish(data []byte) {
	rs.messages <- data
}

func publishToRedis(conn redis.Conn, data []byte, redisChannelName string) error {
	if err := conn.Send("PUBLISH", redisChannelName, data); err != nil {
		return errors.Wrap(err, "Unable to publish message to Redis")
	}
	if err := conn.Flush(); err != nil {
		return errors.Wrap(err, "Unable to flush published message to Redis")
	}
	return nil
}
