import { readdir, readFile, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'

const packageRoot = dirname(new URL(import.meta.resolve('@typeonce/effect-machine-devtools/package.json')).pathname)

const patchFile = async (path, original, replacement) => {
	const source = await readFile(path, 'utf8')
	if (source.includes(replacement)) return
	if (!source.includes(original)) throw new Error(`Could not find the expected Effect Machine layout check in ${path}`)
	await writeFile(path, source.replace(original, replacement))
}

await patchFile(
	join(packageRoot, 'src/internal/browser/chart-layout.ts'),
	'valid: issues.length === 0',
	'valid: issues.every((issue) => issue.code === "label-route-overlap")',
)

const assetsDirectory = join(packageRoot, 'dist/client/assets')
const browserBundle = (await readdir(assetsDirectory)).find((file) => file.startsWith('index-') && file.endsWith('.js'))
if (browserBundle === undefined) throw new Error('Could not find the Effect Machine browser bundle')

await patchFile(
	join(assetsDirectory, browserBundle),
	'valid:n.length===0',
	'valid:n.every(e=>e.code==="label-route-overlap")',
)
