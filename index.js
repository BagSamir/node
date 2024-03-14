import fs from 'fs'
import path from 'path'
import crypto from 'crypto'
import readLine from 'readline'
import { pipeline } from 'node:stream/promises'

const __dirname = process.cwd()
const fileName = path.join(__dirname, '/test/file.log')
const BUFFER_CAPACITY = 100000;
const MAX_ELEM = 1000000;

async function sortFileChunk(tmpChunk, tpmFileNames) {
  const sortChunk = []
  for (const el of tmpChunk) {
    if (el.trim() !== '') {
      sortChunk.push(el)
    }
  }
  console.log('Complete sort')
  const fileSortName = `file_sort_${crypto.randomUUID()}`
  tpmFileNames.push(fileSortName)
  console.log(`creating tmp file: ${fileSortName}`);
  const wr = fs.createWriteStream(`./test/sortFile/${fileSortName}`, { highWaterMark: BUFFER_CAPACITY })
  wr.on('finish', () => {
    console.log('Compression complete.');
  });
  wr.on('error', (err) => {
    console.error('An error occurred during compression:', err);
  });
  const a = sortChunk.map((e) => `${e}\n`);
  await pipeline(a, wr);
  sortChunk.length = 0;
  console.log('written');
}

async function extendSort(fileName) {
  const file = fs.createReadStream(fileName, {
    highWaterMark: BUFFER_CAPACITY
  })
  file.on('end', () => {
    console.log('Data reading complete.');
  });

  file.on('error', (err) => {
    console.error(`Error occurred: ${err}`);
  });

  const lines = readLine.createInterface({
    input: file,
    crlfDelay: Infinity
  })

  const tmpChunk = []
  const tpmFileNames = []
  let size = 0

  for await (const line of lines) {
    size += line.length
    tmpChunk.push(line)
    if (size > MAX_ELEM) {
      console.log('sorting', tmpChunk.length, 'elements', size, 'bytes');
      await sortFileChunk(tmpChunk, tpmFileNames)
      size = 0
    }
  }
  if (tmpChunk.length > 0) {
    await sortFileChunk(tmpChunk, tpmFileNames)
  }
}

extendSort(fileName)