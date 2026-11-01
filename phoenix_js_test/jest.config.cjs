module.exports = {
  rootDir: __dirname,
  clearMocks: true,
  setupFiles: [`${__dirname}/setup.cjs`],
  coverageProvider: "v8",
  testEnvironment: "jest-environment-jsdom",
  testEnvironmentOptions: {
    url: "https://example.com",
  },
  testRegex: "/test/.*_test\\.js$",
  moduleFileExtensions: ["ts", "js", "mjs", "cjs", "json"],
  transform: { "\\.[cm]?[jt]s$": ["babel-jest", { configFile: `${__dirname}/babel.config.json` }] },
  transformIgnorePatterns: [],
}
