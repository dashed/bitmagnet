import {
  TorrentsSearchController,
  defaultOrderBy,
  TorrentSearchControls,
  SizeRangeFilter,
} from "./torrents-search.controller";

describe("TorrentsSearchController", () => {
  let controller: TorrentsSearchController;

  beforeEach(() => {
    controller = new TorrentsSearchController({
      limit: 20,
      page: 1,
      contentType: null,
      orderBy: defaultOrderBy,
      facets: {
        genre: { active: false },
        language: { active: false },
        fileType: { active: false },
        torrentSource: { active: false },
        torrentTag: { active: false },
        videoResolution: { active: false },
        videoSource: { active: false },
      },
    });
  });

  describe("setSizeRange", () => {
    it("should set the size range and update controls", () => {
      // Set a size range
      controller.setSizeRange(1024, 1048576);

      // Get the current controls
      let currentControls = {} as TorrentSearchControls;
      const subscription = controller.controls$.subscribe({
        next: (controls: TorrentSearchControls) => {
          currentControls = controls;
        },
      });

      // Check if sizeRange property is set correctly
      const sizeRange = currentControls.sizeRange as SizeRangeFilter;
      expect(sizeRange).toEqual({ min: 1024, max: 1048576 });

      // Check that page is reset to 1
      expect(currentControls.page).toBe(1);

      // Setting only min
      controller.setSizeRange(1024, undefined);
      const minOnlyRange = currentControls.sizeRange as SizeRangeFilter;
      expect(minOnlyRange).toEqual({ min: 1024, max: undefined });

      // Setting only max
      controller.setSizeRange(undefined, 1048576);
      const maxOnlyRange = currentControls.sizeRange as SizeRangeFilter;
      expect(maxOnlyRange).toEqual({
        min: undefined,
        max: 1048576,
      });

      // Setting to undefined should remove the size range filter
      controller.setSizeRange(undefined, undefined);
      expect(currentControls.sizeRange).toBeUndefined();

      subscription.unsubscribe();
    });

    it("should not set a size range if both min and max are undefined", () => {
      controller.setSizeRange(undefined, undefined);

      let currentControls = {} as TorrentSearchControls;
      const subscription = controller.controls$.subscribe({
        next: (controls: TorrentSearchControls) => {
          currentControls = controls;
        },
      });

      expect(currentControls.sizeRange).toBeUndefined();

      subscription.unsubscribe();
    });

    it("should correctly convert query parameters to size range", () => {
      // Manually trigger an update with these params
      // Note: This is typically handled by the paramsToControls function
      controller.update(() => ({
        limit: 20,
        page: 1,
        contentType: null,
        orderBy: defaultOrderBy,
        facets: {
          genre: { active: false },
          language: { active: false },
          fileType: { active: false },
          torrentSource: { active: false },
          torrentTag: { active: false },
          videoResolution: { active: false },
          videoSource: { active: false },
        },
        sizeRange: {
          min: 100 * 1024 * 1024, // 100 MiB in bytes
          max: 1000 * 1024 * 1024 * 1024, // 1000 GiB in bytes
        },
      }));

      let currentControls = {} as TorrentSearchControls;
      const subscription = controller.controls$.subscribe({
        next: (controls: TorrentSearchControls) => {
          currentControls = controls;
        },
      });

      // Check if values were converted correctly
      expect(currentControls.sizeRange).toBeDefined();
      const sizeRange = currentControls.sizeRange as SizeRangeFilter;
      expect(sizeRange.min).toBe(100 * 1024 * 1024);
      expect(sizeRange.max).toBe(1000 * 1024 * 1024 * 1024);

      subscription.unsubscribe();
    });
  });

  describe("setPublishedAt", () => {
    it("should update controls with publishedAt time frame", () => {
      controller.setPublishedAt("7d");

      let currentControls: TorrentSearchControls = {} as TorrentSearchControls;
      const subscription = controller.controls$.subscribe((controls) => {
        currentControls = controls;
      });

      expect(currentControls.publishedAt).toBe("7d");
      expect(currentControls.page).toBe(1); // Should reset to page 1

      subscription.unsubscribe();
    });

    it("should update controls with special time frame", () => {
      controller.setPublishedAt("this month");

      let currentControls: TorrentSearchControls = {} as TorrentSearchControls;
      const subscription = controller.controls$.subscribe((controls) => {
        currentControls = controls;
      });

      expect(currentControls.publishedAt).toBe("this month");

      subscription.unsubscribe();
    });

    it("should update controls with date range", () => {
      controller.setPublishedAt("2023-01-01 to 2023-01-31");

      let currentControls: TorrentSearchControls = {} as TorrentSearchControls;
      const subscription = controller.controls$.subscribe((controls) => {
        currentControls = controls;
      });

      expect(currentControls.publishedAt).toBe("2023-01-01 to 2023-01-31");

      subscription.unsubscribe();
    });

    it("should remove publishedAt when value is undefined or empty", () => {
      // First set a time frame
      controller.setPublishedAt("7d");

      let currentControls: TorrentSearchControls = {} as TorrentSearchControls;
      const subscription = controller.controls$.subscribe((controls) => {
        currentControls = controls;
      });

      expect(currentControls.publishedAt).toBe("7d");

      // Then clear it
      controller.setPublishedAt(undefined);
      expect(currentControls.publishedAt).toBeUndefined();

      // Set it again and clear with empty string
      controller.setPublishedAt("30d");
      expect(currentControls.publishedAt).toBe("30d");

      controller.setPublishedAt("");
      expect(currentControls.publishedAt).toBeUndefined();

      subscription.unsubscribe();
    });
  });

  describe("controlsToQueryVariables", () => {
    it("should add publishedAt to facets when set", (done) => {
      let foundPublishedAt = false;
      let checkCount = 0;

      // Set up subscription
      const subscription = controller.params$.subscribe((params) => {
        checkCount++;

        // Check structure exists
        expect(params).toBeDefined();
        expect(params.input).toBeDefined();
        expect(params.input.facets).toBeDefined();

        // Type assertion for accessing facets properties
        // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-assignment
        const facets = params.input.facets as any;

        // On the second emission, we should have publishedAt
        if (checkCount === 2) {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
          expect(facets.publishedAt).toBe("7d");
          foundPublishedAt = true;
          done(); // Signal test completion
        }
      });

      // Initial check should not have publishedAt

      // Now set the published date - this should trigger the params$ observable
      controller.setPublishedAt("7d");

      // Cleanup subscription in case the test times out
      setTimeout(() => {
        subscription.unsubscribe();
        if (!foundPublishedAt) {
          done.fail("Timeout: did not find publishedAt in facets");
        }
      }, 2000);
    });

    it("should set the correct value of publishedAt in facets", (done) => {
      const testValue = "this month";
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let latestParams: any = null;

      // Set up subscription
      const subscription = controller.params$.subscribe((params) => {
        latestParams = params;
      });

      // Set the published date
      controller.setPublishedAt(testValue);

      // Check the result
      setTimeout(() => {
        subscription.unsubscribe();
        try {
          expect(latestParams).toBeDefined();
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
          expect(latestParams.input.facets.publishedAt).toBe(testValue);
          done();
        } catch (e) {
          done.fail(e instanceof Error ? e.message : String(e));
        }
      }, 500);
    });
  });
});
