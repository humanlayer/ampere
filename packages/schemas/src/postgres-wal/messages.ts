import { Schema } from 'effect'

import { BeginMessage } from './messages/begin.ts'
import { CommitMessage } from './messages/commit.ts'
import { DeleteMessage } from './messages/delete.ts'
import { InsertMessage } from './messages/insert.ts'
import { LogicalDecodingMessage } from './messages/message.ts'
import { OriginMessage } from './messages/origin.ts'
import { RelationMessage } from './messages/relation.ts'
import { TruncateMessage } from './messages/truncate.ts'
import { TypeMessage } from './messages/type.ts'
import { UpdateMessage } from './messages/update.ts'

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
