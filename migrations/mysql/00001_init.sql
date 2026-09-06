-- +goose Up
-- utf8mb4_bin: ids, client_msg_id and username are case-sensitive keys, matching PostgreSQL.
CREATE TABLE users (
    id            varchar(64)  NOT NULL,
    username      varchar(64)  NOT NULL DEFAULT '',
    password_hash varchar(255) NOT NULL DEFAULT '',
    nickname      varchar(255) NOT NULL DEFAULT '',
    avatar        varchar(1024) NOT NULL DEFAULT '',
    extra         text         NOT NULL,
    created_at      datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at      datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    KEY idx_users_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE online_conns (
    conn_id      varchar(64) NOT NULL,
    user_id      varchar(64) NOT NULL,
    platform_id  int         NOT NULL,
    node_id      varchar(64) NOT NULL,
    heartbeat_at    datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (conn_id),
    KEY idx_online_conns_user (user_id),
    KEY idx_online_conns_node (node_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE chat_groups (
    id           varchar(64)  NOT NULL,
    name         varchar(255) NOT NULL DEFAULT '',
    avatar       varchar(1024) NOT NULL DEFAULT '',
    introduction varchar(1024) NOT NULL DEFAULT '',
    owner_id     varchar(64)  NOT NULL,
    status       int          NOT NULL DEFAULT 0,
    extra        text         NOT NULL,
    created_at      datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at      datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE group_members (
    group_id        varchar(64)  NOT NULL,
    user_id         varchar(64)  NOT NULL,
    role            int          NOT NULL DEFAULT 1,
    nickname        varchar(255) NOT NULL DEFAULT '',
    inviter_user_id varchar(64)  NOT NULL DEFAULT '',
    joined_at       datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (group_id, user_id),
    KEY idx_group_members_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE conversations (
    conversation_id varchar(256) NOT NULL,
    type            int          NOT NULL,
    group_id        varchar(64)  NOT NULL DEFAULT '',
    max_seq         bigint       NOT NULL DEFAULT 0,
    created_at      datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at      datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (conversation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

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
    is_pinned       tinyint(1)   NOT NULL DEFAULT 0,
    extra           text         NOT NULL,
    updated_at      datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    created_at      datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (owner_id, conversation_id),
    KEY idx_user_conversations_list (owner_id, updated_at DESC, conversation_id DESC),
    KEY idx_user_conversations_conv (conversation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

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
    send_time       datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    created_at      datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (conversation_id, seq),
    UNIQUE KEY uk_messages_server_msg_id (server_msg_id),
    UNIQUE KEY uk_messages_client_msg_id (conversation_id, sender_id, client_msg_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- +goose Down
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS user_conversations;
DROP TABLE IF EXISTS conversations;
DROP TABLE IF EXISTS group_members;
DROP TABLE IF EXISTS chat_groups;
DROP TABLE IF EXISTS online_conns;
DROP TABLE IF EXISTS users;
