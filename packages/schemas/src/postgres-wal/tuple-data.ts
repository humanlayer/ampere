import { Effect, Schema } from 'effect'

import type { PgOutputBytesCursor } from './bytes-cursor'
import { failPgOutputDecode, PgOutputDecodeFailure } from './errors'
import type { PgOutputV1MessageTypeName } from './type-bytes'

export const TupleCellKindByte = {
	Null: 0x6e,
	UnchangedToast: 0x75,
	Text: 0x74,
	Binary: 0x62,
} as const

export const TupleDataKindByte = {
	New: 0x4e,
	Old: 0x4f,
	Key: 0x4b,
} as const

export const TupleCell = Schema.TaggedUnion({
	Null: {},
	UnchangedToast: {},
	Text: { bytes: Schema.Uint8Array },
	Binary: { bytes: Schema.Uint8Array },
})
export type TupleCell = typeof TupleCell.Type

export const TupleData = Schema.Array(TupleCell)
export type TupleData = typeof TupleData.Type

const failTupleDecode = (
	reason: 'truncated-message' | 'unknown-tuple-cell-kind',
	typeByte: number,
	messageTypeName: PgOutputV1MessageTypeName,
	message: string,
) =>
	failPgOutputDecode(
		PgOutputDecodeFailure.cases.MessageDecodeError.make({
			reason,
			typeByte,
			messageTypeName,
		}),
		message,
	)

export const readTupleDataFromCursor = (
	cursor: PgOutputBytesCursor,
	typeByte: number,
	messageTypeName: PgOutputV1MessageTypeName,
) =>
	Effect.gen(function* () {
		const columnCount = cursor.readUint16()
		if (columnCount === undefined) {
			return yield* failTupleDecode(
				'truncated-message',
				typeByte,
				messageTypeName,
				`${messageTypeName} message is missing a tuple column count.`,
			)
		}

		const cells: Array<TupleCell> = []
		for (let columnIndex = 0; columnIndex < columnCount; columnIndex += 1) {
			const cellKind = cursor.readUint8()
			if (cellKind === undefined) {
				return yield* failTupleDecode(
					'truncated-message',
					typeByte,
					messageTypeName,
					`${messageTypeName} message is truncated inside tuple data.`,
				)
			}

			if (cellKind === TupleCellKindByte.Null) {
				cells.push(TupleCell.cases.Null.make({}))
				continue
			}
			if (cellKind === TupleCellKindByte.UnchangedToast) {
				cells.push(TupleCell.cases.UnchangedToast.make({}))
				continue
			}
			if (cellKind === TupleCellKindByte.Text || cellKind === TupleCellKindByte.Binary) {
				const valueLength = cursor.readUint32()
				if (valueLength === undefined) {
					return yield* failTupleDecode(
						'truncated-message',
						typeByte,
						messageTypeName,
						`${messageTypeName} message is missing a tuple cell length.`,
					)
				}
				const valueBytes = cursor.readBytes(valueLength)
				if (valueBytes === undefined) {
					return yield* failTupleDecode(
						'truncated-message',
						typeByte,
						messageTypeName,
						`${messageTypeName} message is truncated inside a tuple cell.`,
					)
				}
				cells.push(
					cellKind === TupleCellKindByte.Text
						? TupleCell.cases.Text.make({ bytes: valueBytes })
						: TupleCell.cases.Binary.make({ bytes: valueBytes }),
				)
				continue
			}

			return yield* failTupleDecode(
				'unknown-tuple-cell-kind',
				typeByte,
				messageTypeName,
				`${messageTypeName} message has an unknown tuple cell kind.`,
			)
		}

		return cells
	})
