package cluster

type PushContent struct {
	Text   string `json:"text,omitempty"`
	Image  string `json:"image,omitempty"`
	Video  string `json:"video,omitempty"`
	Audio  string `json:"audio,omitempty"`
	File   string `json:"file,omitempty"`
	Custom string `json:"custom,omitempty"`
}

// PushMessage is the stable cross-instance realtime payload.
type PushMessage struct {
	ServerMsgId    int64       `json:"server_msg_id"`
	ConversationId string      `json:"conversation_id"`
	Seq            int64       `json:"seq"`
	ClientMsgId    string      `json:"client_msg_id"`
	SenderId       string      `json:"sender_id"`
	RecvId         string      `json:"recv_id,omitempty"`
	GroupId        string      `json:"group_id,omitempty"`
	SessionType    int32       `json:"session_type"`
	MsgType        int32       `json:"msg_type"`
	Content        PushContent `json:"content"`
	SendAt         int64       `json:"send_at"`
}

// PushPayload wraps the message body for envelope compatibility.
type PushPayload struct {
	Message *PushMessage `json:"message"`
}

// PushEnvelope is the route-only cluster delivery envelope.
type PushEnvelope struct {
	Version        int                  `json:"version"`
	PushId         string               `json:"push_id"`
	SourceInstance string               `json:"source_instance"`
	SentAt         int64                `json:"sent_at"`
	TargetConnMap  map[string][]ConnRef `json:"target_conn_map"`
	Payload        *PushPayload         `json:"payload"`
}
