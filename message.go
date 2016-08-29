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
	Handle string `json:"handle"`
	User   string `json:"user"`
	Room   string `json:"room"`
	Text   string `json:"text"`
}

func init() {
	var err error
	WaitingMessage, err = json.Marshal(message{
		Handle: "system.Message",
		Text:   "Waiting for redis to be available. Messaging won't work until redis is available",
	})
	if err != nil {
		panic(err)
	}

	AvailableMessage, err = json.Marshal(message{
		Handle: "system.Message",
		Text:   "Redis is now available & messaging is now possible",
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

	if msg.Handle == "" && msg.Text == "" {
		return msg, errors.New("Message has no Handle or Text")
	}

	return msg, nil
}
