import { Schema } from 'effect'

import { BeginMessage } from './messages/begin'
import { CommitMessage } from './messages/commit'
import { DeleteMessage } from './messages/delete'
import { InsertMessage } from './messages/insert'
import { LogicalDecodingMessage } from './messages/message'
import { OriginMessage } from './messages/origin'
import { RelationMessage } from './messages/relation'
import { TruncateMessage } from './messages/truncate'
import { TypeMessage } from './messages/type'
import { UpdateMessage } from './messages/update'

export const PgOutputMessage = Schema.Union([
	BeginMessage,
	CommitMessage,
	OriginMessage,
	RelationMessage,
	TypeMessage,
	InsertMessage,
	UpdateMessage,
	DeleteMessage,
	TruncateMessage,
	LogicalDecodingMessage,
])
export type PgOutputMessage = typeof PgOutputMessage.Type
