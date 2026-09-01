const textDecoder = new TextDecoder()

export const createPgOutputBytesCursor = (bytes: Uint8Array, startOffset = 0) => {
	const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
	let offset = startOffset

	const hasBytes = (count: number) => offset + count <= bytes.byteLength

	return {
		get offset() {
			return offset
		},
		remaining: () => bytes.byteLength - offset,
		hasRemaining: () => offset < bytes.byteLength,
		readUint8: (): number | undefined => {
			if (!hasBytes(1)) {
				return undefined
			}
			const value = view.getUint8(offset)
			offset += 1
			return value
		},
		readUint16: (): number | undefined => {
			if (!hasBytes(2)) {
				return undefined
			}
			const value = view.getUint16(offset, false)
			offset += 2
			return value
		},
		readUint32: (): number | undefined => {
			if (!hasBytes(4)) {
				return undefined
			}
			const value = view.getUint32(offset, false)
			offset += 4
			return value
		},
		readInt32: (): number | undefined => {
			if (!hasBytes(4)) {
				return undefined
			}
			const value = view.getInt32(offset, false)
			offset += 4
			return value
		},
		readBigUint64: (): bigint | undefined => {
			if (!hasBytes(8)) {
				return undefined
			}
			const value = view.getBigUint64(offset, false)
			offset += 8
			return value
		},
		readBytes: (length: number): Uint8Array | undefined => {
			if (!hasBytes(length)) {
				return undefined
			}
			const value = bytes.slice(offset, offset + length)
			offset += length
			return value
		},
		readNullTerminatedString: (): string | undefined => {
			let end = offset
			while (end < bytes.byteLength && bytes[end] !== 0) {
				end += 1
			}
			if (end === bytes.byteLength) {
				return undefined
			}
			const value = textDecoder.decode(bytes.subarray(offset, end))
			offset = end + 1
			return value
		},
	}
}
export type PgOutputBytesCursor = ReturnType<typeof createPgOutputBytesCursor>
