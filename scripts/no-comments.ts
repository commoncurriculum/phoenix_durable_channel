/// <reference types="node" />
import { execFileSync } from "node:child_process"
import { readFileSync } from "node:fs"
import ts from "typescript"

const vendored = /^(phoenix_js_test\/test\/|test\/phoenix\/|test\/support\/)/
const directive = /^\/\/\/ <reference /

const files = execFileSync("git", ["ls-files", "*.ts", "*.js", "*.mjs", "*.cjs"], {
  encoding: "utf8",
})
  .split("\n")
  .filter((file) => file !== "" && !vendored.test(file))

function comments(file: string): Array<string> {
  const text = readFileSync(file, "utf8")
  const source = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true)
  const found = new Map<number, string>()
  const visit = (node: ts.Node): void => {
    for (const range of [
      ...(ts.getLeadingCommentRanges(text, node.pos) ?? []),
      ...(ts.getTrailingCommentRanges(text, node.end) ?? []),
    ])
      found.set(range.pos, text.slice(range.pos, range.end))
    node.getChildren(source).forEach(visit)
  }
  visit(source)
  return [...found.values()]
    .filter((comment) => !directive.test(comment))
    .map((comment) => `${file}: ${comment}`)
}

const commented = files.flatMap(comments)

if (commented.length > 0) {
  console.error(`Comments are not allowed; say it in a test instead:\n${commented.join("\n")}`)
  process.exit(1)
}
