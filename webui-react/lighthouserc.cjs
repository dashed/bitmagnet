module.exports = {
  ci: {
    collect: {
      numberOfRuns: 3,
      settings: {
        onlyCategories: ["performance"],
      },
      startServerCommand: "pnpm exec vite preview --host 127.0.0.1 --port 4174",
      startServerReadyPattern: "Local:",
      startServerReadyTimeout: 30000,
      url: ["http://127.0.0.1:4174/app/"],
    },
    assert: {
      assertions: {
        "largest-contentful-paint": ["error", { maxNumericValue: 2500 }],
        "total-blocking-time": ["error", { maxNumericValue: 200 }],
      },
    },
    upload: {
      outputDir: ".lighthouseci",
      target: "filesystem",
    },
  },
};
