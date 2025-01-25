/** @type {import('ts-jest').JestConfigWithTsJest} **/
module.exports = {
  roots: ['<rootDir>/src/', '<rootDir>/test/'],
  moduleFileExtensions: ['ts', 'tsx', 'js'],
  testEnvironment: "node",
  transform: {
    "^.+.tsx?$": ["ts-jest",{}],
  },
};
