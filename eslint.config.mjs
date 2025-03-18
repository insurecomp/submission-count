import prettier from "eslint-config-prettier";

export default [
  {
    ignores: ["node_modules/", "dist/"], // Ignore unnecessary folders
  },
  {
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: {
        console: "readonly",
        process: "readonly",
        __dirname: "readonly",
        module: "readonly",
      },
    },
    plugins: {},
    rules: {
      "no-unused-vars": "warn",
      "no-console": "off",
      "no-var": "error",
      "prefer-const": "warn",
    },
  },
  prettier,
];
