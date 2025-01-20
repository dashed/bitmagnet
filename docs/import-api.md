# Import API

The import API endpoint allows you to import torrent metadata into bitmagnet from external sources.

## Endpoint Details

- **Method**: POST
- **URL**: `/import`
- **Content-Type**: `application/json`
- **Optional Headers**: 
  - `x-import-id`: A unique identifier for the import batch
    - If not provided, defaults to current Unix timestamp

## Request Format

The request body must be newline-delimited JSON (NDJSON), where each line is a valid JSON object representing a torrent item.

### Item Schema

```typescript
{
  // Required Fields
  source: string,            // Source identifier for the torrent (e.g., "rarbg")
  infoHash: string,         // 20-byte torrent info hash in hex format
  name: string,             // Name of the torrent
  size: number,             // Size in bytes
  
  // Optional Fields
  private: boolean,         // Whether the torrent is private
  
  // Content Classification
  contentType: string,      // Type of content, one of:
                           // "movie", "tv_show", "music", "ebook", "comic", 
                           // "audiobook", "game", "software", "xxx"
  
  // External Content References
  contentSource: string,    // External content source (e.g., "imdb", "tmdb")
  contentId: string,        // ID from the external content source
  title: string,           // Title of the content
  releaseDate: string,     // ISO date string (YYYY-MM-DD)
  releaseYear: number,     // Year of release
  
  // TV Show Specific Fields
  episodes: {              
    seasons: [{
      number: number,      // Season number
      episodes: number[]   // Array of episode numbers
    }]
  },
  
  // Video-specific Fields (for movies/tv shows)
  videoResolution: string,  // One of:
                           // "V360p", "V480p", "V540p", "V576p", "V720p",
                           // "V1080p", "V1440p", "V2160p", "V4320p"
  
  videoSource: string,     // One of:
                           // "CAM", "TELESYNC", "TELECINE", "WORKPRINT",
                           // "DVD", "TV", "WEBDL", "WEBRip", "BluRay"
  
  videoCodec: string,      // One of:
                           // "H264", "x264", "x265", "XviD", "DivX",
                           // "MPEG2", "MPEG4"
  
  video3D: string,         // One of:
                           // "V3D", "V3DSBS", "V3DOU"
  
  videoModifier: string,   // One of:
                           // "REGIONAL", "SCREENER", "RAWHD",
                           // "BRDISK", "REMUX"
  
  releaseGroup: string,    // Name of the release group
  
  publishedAt: string      // ISO datetime string of when the torrent was published
}
```

### Example Request

```bash
curl -X POST http://localhost:3333/import \
  -H "Content-Type: application/json" \
  -H "x-import-id: my-import-1" \
  --data-binary @- << 'EOF'
{"source":"rarbg","infoHash":"e9776f2c2cb5d8842a4c40bfb9b9d3b3b3b3b3b3","name":"Big.Buck.Bunny.2008.1080p.BluRay.x264","size":1500000000,"contentType":"movie","videoResolution":"V1080p","videoSource":"BluRay","videoCodec":"x264","publishedAt":"2023-01-01T00:00:00.000Z"}
{"source":"rarbg","infoHash":"f8665e1b1ba4c7731b3a3a2a2a2a2a2a2a2a2a2a","name":"Ubuntu.22.04.LTS.Desktop.amd64","size":3000000000,"contentType":"software","publishedAt":"2023-01-02T00:00:00.000Z"}
EOF
```

## Response

The endpoint provides real-time feedback during the import process:

- Progress updates are sent every 1,000 items
- The response is flushed every 10,000 items
- On completion, returns total count of imported items

### Success Response
- **Status**: 200 OK
- **Content**: Text updates showing progress and final count

Example:
```
1000 items imported
2000 items imported
...
10000 items imported
...
50000 items imported
import complete
```

### Error Response
- **Status**: 400 Bad Request
- **Content**: Error message describing what went wrong

## Notes

1. Each line in the request body must be a complete, valid JSON object
2. The `infoHash` must be a 20-byte hex-encoded string
3. All enum fields (contentType, videoResolution, etc.) are case-sensitive
4. Dates should be in ISO format
   - `releaseDate`: YYYY-MM-DD
   - `publishedAt`: ISO 8601 with timezone
5. The import is processed asynchronously with real-time progress updates
6. If an imported torrent is later discovered by the DHT crawler, its file information will be updated
7. Items are processed in batches for better performance
8. Each import creates a queue job for further metadata enrichment
9. The import ID can be used to track and filter imports from different sources

## Common Use Cases

1. **Importing from RARBG backup**:
   - Map categories to content types
   - Extract video quality information
   - Include IMDB IDs when available

2. **Importing from torrent files**:
   - Extract basic metadata
   - Let the classifier handle content type detection

3. **Importing from other torrent sites**:
   - Map site-specific categories
   - Include site-specific IDs in source information 