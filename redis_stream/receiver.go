package redis_stream

import (
	"sync"
	"time"

	"github.com/sangchul-sim/webapp-golang-beego/chat"

	"github.com/astaxie/beego"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// RedisReceiver receives messages from Redis and broadcasts them to all
// registered websocket connections that are Registered.
type RedisReceiver struct {
	pool  *redis.Pool
	mu    sync.Mutex
	conns map[string]*websocket.Conn
}

// NewRedisReceiver creates a RedisReceiver that will use the provided
// redis.Pool.
// 구조체 생성자 패턴
func NewRedisReceiver(pool *redis.Pool) *RedisReceiver {
	return &RedisReceiver{
		pool:  pool,
		conns: make(map[string]*websocket.Conn),
	}
}

// TODO
// 언제 사용되는지 확인할 것
func (rr *RedisReceiver) Wait(_ time.Time) error {
	rr.Broadcast(chat.WaitingMessage)
	time.Sleep(chat.WaitSleep)
	return nil
}

// Run receives pubsub messages from Redis after establishing a connection.
// When a valid message is received it is broadcast to all connected websockets
func (rr *RedisReceiver) Run(redisChannelName string) error {
	conn := rr.pool.Get()
	defer conn.Close()
	psc := redis.PubSubConn{Conn: conn}
	psc.Subscribe(redisChannelName)
	for {
		switch v := psc.Receive().(type) {
		// Message represents a message notification.
		// redis.Message{Channel string, Data []byte}
		case redis.Message:
			beego.Info("Redis Message Received", string(v.Data))
			// msg : chat.message
			msg, err := chat.ValidateMessage(v.Data)
			if err != nil {
				beego.Error("Error unmarshalling message from Redis", err)
				continue
			}

			// TODO
			// socket.broadcast.to(socket.user.room_id).emit() 은 어떻게 구현할 것인가?
			// room 에게만 broadcast message 보내기
			// user 에게만 direct message 보내기 (해당 room 에서만)
			if msg.Handle == "system.message" || msg.Handle == "room.message" {
				rr.Broadcast(v.Data)
			} else {
				// rr.Broadcast(v.Data)
				// 어떻게 처리할 것인가?
				continue
			}

		// redis.Subscription{Kind string, Channel string, Count int}
		case redis.Subscription:
			beego.Info("Redis Subscription Received kind:", v.Kind, " count:", v.Count)
		case error:
			return errors.Wrap(v, "Error while subscribed to Redis channel")
		default:
			beego.Info("Unknown Redis receive during subscription", v)
		}
	}
}

// Broadcast the provided message to all connected websocket connections.
// If an error occurs while writting a message to a websocket connection it is
// closed and deregistered.
//
// data는 byte 타입의 slice
// slice 는 레퍼런스 타입
func (rr *RedisReceiver) Broadcast(data []byte) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	for id, conn := range rr.conns {
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			LogData := make(map[string]interface{})
			LogData["id"] = id
			LogData["data"] = data
			LogData["err"] = err

			beego.Error("Error writting data to connection! Closing and removing Connection", LogData)
		}
	}
}

// Register the websocket connection with the receiver and return a unique
// identifier for the connection. This identifier can be used to deregister the
// connection later
func (rr *RedisReceiver) Register(conn *websocket.Conn) string {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	id := uuid.NewV4().String()
	rr.conns[id] = conn
	return id
}

// DeRegister the connection by closing it and removing it from our list.
func (rr *RedisReceiver) DeRegister(id string) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	conn, ok := rr.conns[id]
	if ok {
		conn.Close()
		delete(rr.conns, id)
	}
}
