const { readFileSync, writeFileSync } = require("node:fs")
const { join } = require("node:path")

const listPath = join(__dirname, "internals.json")

class InternalsReporter {
  onRunComplete(_contexts, results) {
    const internals = JSON.parse(readFileSync(listPath, "utf8"))
    const errors = new Map()
    const failed = results.testResults.flatMap((file) => {
      const suite = file.testFilePath.split("/").pop()
      const tests = file.testResults
        .filter((test) => test.status === "failed")
        .map((test) => `${suite} › ${test.fullName}`)
      for (const test of file.testResults)
        if (test.status === "failed")
          errors.set(`${suite} › ${test.fullName}`, test.failureMessages[0]?.split("\n")[0] ?? "")
      if (file.testExecError)
        errors.set(`${suite} › suite`, file.testExecError.message.split("\n")[0])
      return file.testExecError ? [`${suite} › suite`] : tests
    })
    if (process.env.PHOENIX_JS_TEST_RECORD) {
      writeFileSync(
        listPath,
        `${JSON.stringify(
          Object.fromEntries(failed.map((name) => [name, internals[name] || errors.get(name)])),
          null,
          2,
        )}\n`,
      )
      return
    }
    const unexpected = failed.filter((name) => !(name in internals))
    const nowPassing = Object.keys(internals).filter((name) => !failed.includes(name))
    for (const name of unexpected) console.error(`FAILED, not a phoenix.js internals test: ${name}`)
    for (const name of nowPassing)
      console.error(`PASSES now, remove it from internals.json: ${name}`)
    console.log(
      `Phoenix's JS tests: ${results.numPassedTests} pass, ${failed.length - unexpected.length} reach into phoenix.js internals`,
    )
    if (unexpected.length > 0 || nowPassing.length > 0) process.exitCode = 1
  }
}

module.exports = InternalsReporter
