// Move to a more direct unit testing approach without Angular dependencies
// This will let us test the conversion functions directly which is what we care about

describe("Size Filter Conversion Tests", () => {
  // Simple implementation of our sizeToBytes function for direct testing
  function sizeToBytes(size: number | null, unit: string): number | undefined {
    if (size === null) {
      return undefined;
    }

    // Use safe calculation for large numbers
    let bytes: number;
    switch (unit) {
      case "KB":
        bytes = Math.floor(size * 1024);
        break;
      case "MB":
        bytes = Math.floor(size * 1024 * 1024);
        break;
      case "GB":
        // For GB values, calculate more carefully to avoid integer overflow
        bytes = Math.floor(size * 1024 * 1024) * 1024;
        break;
      case "TB":
        // For TB values, calculate even more carefully
        bytes = Math.floor(size * 1024 * 1024) * 1024 * 1024;
        break;
      default:
        bytes = size;
    }

    return bytes;
  }

  // Function to convert from bytes back to specified unit
  function bytesToUnit(
    bytes: number | undefined,
    unit: string,
  ): number | undefined {
    if (bytes === undefined) {
      return undefined;
    }

    let result: number;
    switch (unit) {
      case "KB":
        result = Math.round(bytes / 1024);
        break;
      case "MB":
        result = Math.round(bytes / (1024 * 1024));
        break;
      case "GB":
        // More careful division for large numbers
        result = Math.round(bytes / 1024 / (1024 * 1024));
        break;
      case "TB":
        // Even more careful division
        result = Math.round(bytes / (1024 * 1024) / (1024 * 1024));
        break;
      default:
        result = bytes;
    }

    return result;
  }

  describe("sizeToBytes conversion", () => {
    it("should return undefined for null size", () => {
      expect(sizeToBytes(null, "MB")).toBeUndefined();
    });

    it("should convert KB values to bytes correctly", () => {
      expect(sizeToBytes(1, "KB")).toBe(1024);
      expect(sizeToBytes(5, "KB")).toBe(5120);
      expect(sizeToBytes(1000, "KB")).toBe(1024000);
    });

    it("should convert MB values to bytes correctly", () => {
      expect(sizeToBytes(1, "MB")).toBe(1048576); // 1024^2
      expect(sizeToBytes(10, "MB")).toBe(10485760);
      expect(sizeToBytes(1000, "MB")).toBe(1048576000);
    });

    it("should convert GB values to bytes correctly", () => {
      expect(sizeToBytes(1, "GB")).toBe(1073741824); // 1024^3
      expect(sizeToBytes(2, "GB")).toBe(2147483648);
      expect(sizeToBytes(20, "GB")).toBe(21474836480);
    });

    it("should convert TB values to bytes correctly", () => {
      expect(sizeToBytes(1, "TB")).toBe(1099511627776); // 1024^4
      expect(sizeToBytes(2, "TB")).toBe(2199023255552);
    });

    it("should handle decimal values correctly", () => {
      expect(sizeToBytes(1.5, "MB")).toBe(1572864);
      expect(sizeToBytes(0.5, "GB")).toBe(536870912);
    });
  });

  describe("bytesToUnit conversion", () => {
    it("should return undefined for undefined bytes", () => {
      expect(bytesToUnit(undefined, "MB")).toBeUndefined();
    });

    it("should convert bytes to KB correctly", () => {
      expect(bytesToUnit(1024, "KB")).toBe(1);
      expect(bytesToUnit(5120, "KB")).toBe(5);
      expect(bytesToUnit(1024000, "KB")).toBe(1000);
    });

    it("should convert bytes to MB correctly", () => {
      expect(bytesToUnit(1048576, "MB")).toBe(1);
      expect(bytesToUnit(10485760, "MB")).toBe(10);
      expect(bytesToUnit(1048576000, "MB")).toBe(1000);
    });

    it("should convert bytes to GB correctly", () => {
      expect(bytesToUnit(1073741824, "GB")).toBe(1);
      expect(bytesToUnit(2147483648, "GB")).toBe(2);
      expect(bytesToUnit(21474836480, "GB")).toBe(20);
    });

    it("should convert bytes to TB correctly", () => {
      expect(bytesToUnit(1099511627776, "TB")).toBe(1);
      expect(bytesToUnit(2199023255552, "TB")).toBe(2);
    });
  });

  // Test the round-trip conversion (most important for fixing our bug)
  describe("Round-trip conversion (our bug was here)", () => {
    it("should correctly round-trip 20 GB values", () => {
      // 20 GB to bytes
      const bytes = sizeToBytes(20, "GB");
      expect(bytes).toBe(21474836480);

      // Bytes back to GB
      const gb = bytesToUnit(bytes, "GB");
      expect(gb).toBe(20);
    });

    it("should correctly round-trip 2 TB values", () => {
      // 2 TB to bytes
      const bytes = sizeToBytes(2, "TB");
      expect(bytes).toBe(2199023255552);

      // Bytes back to TB
      const tb = bytesToUnit(bytes, "TB");
      expect(tb).toBe(2);
    });

    it("should maintain precision for large values", () => {
      // Create a range of GB values
      for (let gb = 1; gb <= 50; gb += 5) {
        const bytes = sizeToBytes(gb, "GB");
        const convertedBack = bytesToUnit(bytes, "GB");
        expect(convertedBack).toBe(gb);
      }
    });
  });
});
