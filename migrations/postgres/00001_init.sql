-- +goose Up
CREATE TABLE users (
    id            varchar(64)  PRIMARY KEY,
    username      varchar(64)  NOT NULL DEFAULT '',
    password_hash varchar(255) NOT NULL DEFAULT '',
    nickname      varchar(255) NOT NULL DEFAULT '',
    avatar        varchar(1024) NOT NULL DEFAULT '',
    extra         text         NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_users_username ON users (username);

CREATE TABLE online_conns (
    conn_id      varchar(64) PRIMARY KEY,
    user_id      varchar(64) NOT NULL,
    platform_id  int         NOT NULL,
    node_id      varchar(64) NOT NULL,
    heartbeat_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_online_conns_user ON online_conns (user_id);
CREATE INDEX idx_online_conns_node ON online_conns (node_id);

CREATE TABLE cache (
    key        text        PRIMARY KEY,
    value      text        NOT NULL,
    expires_at timestamptz NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_cache_expires ON cache (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE chat_groups (
    id           varchar(64)  PRIMARY KEY,
    name         varchar(255) NOT NULL DEFAULT '',
    avatar       varchar(1024) NOT NULL DEFAULT '',
    introduction varchar(1024) NOT NULL DEFAULT '',
    owner_id     varchar(64)  NOT NULL,
    status       int          NOT NULL DEFAULT 0,
    extra        text         NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE group_members (
    group_id        varchar(64)  NOT NULL,
    user_id         varchar(64)  NOT NULL,
    role            int          NOT NULL DEFAULT 1,
    nickname        varchar(255) NOT NULL DEFAULT '',
    inviter_user_id varchar(64)  NOT NULL DEFAULT '',
    joined_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, user_id)
);
CREATE INDEX idx_group_members_user ON group_members (user_id);

CREATE TABLE conversations (
    conversation_id varchar(256) PRIMARY KEY,
    type            int          NOT NULL,
    group_id        varchar(64)  NOT NULL DEFAULT '',
    max_seq         bigint       NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE user_conversations (
    owner_id        varchar(64)  NOT NULL,
    conversation_id varchar(256) NOT NULL,
    type            int          NOT NULL,
    peer_user_id    varchar(64)  NOT NULL DEFAULT '',
    group_id        varchar(64)  NOT NULL DEFAULT '',
    min_seq         bigint       NOT NULL DEFAULT 1,
    max_seq         bigint       NOT NULL DEFAULT 0,
    read_seq        bigint       NOT NULL DEFAULT 0,
    recv_msg_opt    int          NOT NULL DEFAULT 0,
    is_pinned       boolean      NOT NULL DEFAULT false,
    extra           text         NOT NULL DEFAULT '',
    updated_at      timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_id, conversation_id)
);
CREATE INDEX idx_user_conversations_list ON user_conversations (owner_id, updated_at DESC, conversation_id DESC);
CREATE INDEX idx_user_conversations_conv ON user_conversations (conversation_id);

CREATE TABLE messages (
    conversation_id varchar(256) NOT NULL,
    seq             bigint       NOT NULL,
    server_msg_id   varchar(64)  NOT NULL,
    client_msg_id   varchar(64)  NOT NULL,
    sender_id       varchar(64)  NOT NULL,
    recv_id         varchar(64)  NOT NULL DEFAULT '',
    group_id        varchar(64)  NOT NULL DEFAULT '',
    session_type    int          NOT NULL,
    content_type    int          NOT NULL,
    content         text         NOT NULL,
    send_time       timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (conversation_id, seq)
);
CREATE UNIQUE INDEX uk_messages_server_msg_id ON messages (server_msg_id);
CREATE UNIQUE INDEX uk_messages_client_msg_id ON messages (conversation_id, sender_id, client_msg_id);

-- +goose Down
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS user_conversations;
DROP TABLE IF EXISTS conversations;
DROP TABLE IF EXISTS group_members;
DROP TABLE IF EXISTS chat_groups;
DROP TABLE IF EXISTS cache;
DROP TABLE IF EXISTS online_conns;
DROP TABLE IF EXISTS users;
