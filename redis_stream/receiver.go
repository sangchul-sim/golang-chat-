package redis_stream

import (
	"strconv"
	"sync"
	"time"

	"github.com/sangchul-sim/webapp-golang-beego/chat"

	"github.com/astaxie/beego"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const (
	SystemMessage = "system.message"
	RoomMessage   = "room.message"
	RoomIn        = "room.in"
)

var (
	redisSender *RedisSender
)

// RedisReceiver receives messages from Redis and broadcasts them to all
// registered websocket connections that are Registered.
type RedisReceiver struct {
	pool  *redis.Pool
	mu    sync.Mutex
	conns map[string]*websocket.Conn
}

func RedisKey(key string) string {
	AppName := beego.AppConfig.String("appname")

	return AppName + ":" + key
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

func (rr *RedisReceiver) Wait(_ time.Time) error {
	rr.Broadcast(chat.WaitingMessage)
	time.Sleep(chat.WaitSleep)
	return nil
}

func (rr *RedisReceiver) HSet(Key string, HashKey interface{}, Val *[]byte) error {
	conn := rr.pool.Get()
	if _, err := conn.Do("HSET", Key, HashKey, *Val); err != nil {
		return err
	}
	return nil
}

func (rr *RedisReceiver) SAdd(Key string, Val interface{}) error {
	conn := rr.pool.Get()
	if _, err := conn.Do("SADD", Key, Val); err != nil {
		return err
	}
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
			beego.Info("msg", msg)
			if err != nil {
				beego.Error("Error unmarshalling message from Redis", err)
				continue
			}

			// TODO
			// socket.broadcast.to(socket.user.room_id).emit() 은 어떻게 구현할 것인가?
			// room 에게만 broadcast message 보내기
			// user 에게만 direct message 보내기 (해당 room 에서만)
			//
			// room.in 구현완료
			switch msg.Handle {
			case SystemMessage, RoomMessage:
				rr.Broadcast(v.Data)
			case RoomIn:
				RoomRedisKey := RedisKey("room")
				UserRedisKey := RedisKey("user")
				RoomRedisKey += ":" + strconv.Itoa(msg.RoomID)

				// 유저 정보를 넣을것
				// JsonMessage, err := json.Marshal(msg)
				// if err == nil {
				// 	err = rr.HSet(RoomRedisKey, msg.UserID, &JsonMessage)
				// 	if err != nil {
				// 		beego.Debug("err", err)
				// 	}
				//
				// 	err = rr.HSet(UserRedisKey, msg.UserID, &JsonMessage)
				// 	if err != nil {
				// 		beego.Debug("err", err)
				// 	}
				// }

				err = rr.SAdd(RoomRedisKey, msg.UserID)
				if err != nil {
					beego.Debug("err", err)
				}

				err = rr.SAdd(UserRedisKey, msg.UserID)
				if err != nil {
					beego.Debug("err", err)
				}

				// hSet(RoomRedisKey, msg.user_id, JSON.stringfy(user_data))
				// zincrby(redis_key, 1, msg.room_id)
				// hSet(UserRedisKey, msg.user_id, JSON.stringfy(user_data))
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
