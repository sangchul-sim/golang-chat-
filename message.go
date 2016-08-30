package chat

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
)

var (
	WaitingMessage, AvailableMessage []byte
	WaitSleep                        = time.Second * 10
)

// message sent to us by the javascript client
type message struct {
	Handle  string `json:"handle"`
	UserID  int    `json:"user_id"`
	RoomID  int    `json:"room_id"`
	Message string `json:"message"`
}

func init() {
	var err error
	WaitingMessage, err = json.Marshal(message{
		Handle:  "system.Message",
		Message: "Waiting for redis to be available. Messaging won't work until redis is available",
	})
	if err != nil {
		panic(err)
	}

	AvailableMessage, err = json.Marshal(message{
		Handle:  "system.Message",
		Message: "Redis is now available & messaging is now possible",
	})
	if err != nil {
		panic(err)
	}
}

func ValidateMessage(data []byte) (message, error) {
	var msg message

	if err := json.Unmarshal(data, &msg); err != nil {
		return msg, errors.Wrap(err, "Unmarshaling message")
	}

	if msg.Handle == "" && msg.Message == "" {
		return msg, errors.New("Message has no Handle or Message")
	}

	return msg, nil
}
