# BitMagnet Development Reference

## Build & Run Commands
- Build Go app: `task build`
- Build WebUI: `task build-webui` or `cd webui && npm run build`
- Run tests: `task test` or `go test -v ./...`
- Run single test: `go test -v -run TestName path/to/package`
- Generate code: `task gen`
- Lint Go: `golangci-lint run --timeout=10m`
- Lint WebUI: `cd webui && npm run lint` / Fix: `npm run lint:fix`
- Migrations: `task migrate` / Create: `task create-migration name=migration_name`
- Docker Dev: `docker-compose -f docker-compose.dev.yml up -d --build` (builds and starts containers)

## Development Workflow
1. **Making UI Changes**:
   - Edit files in `webui/src/app/` directory
   - Run `cd webui && npm run build` to compile Angular app
   - **Important**: Since the WebUI is embedded in the Go binary, you need to rebuild the Docker container:
     - `docker-compose -f docker-compose.dev.yml up -d --build` to rebuild and restart the container with the new UI
   - Access UI at http://localhost:3333/webui/
   - **Note**: Both steps (build WebUI → rebuild container) are required for UI changes to appear in the container
   - The container rebuild automatically includes building the Go binary with the embedded WebUI

2. **Debugging Common Issues**:
   - Check Docker logs with `docker logs bitmagnet-dev`
   - For database issues, use container exec: `docker exec bitmagnet-dev psql [commands]`
   - For frontend issues, check browser console and component structure
   - Check GraphQL schema in `graphql/schema/` directory
   - **WebUI Changes Not Appearing**: If UI changes aren't showing up in the container, verify you've completed both steps:
     1. Rebuilt the WebUI: `cd webui && npm run build` 
     2. Rebuilt the container: `docker-compose -f docker-compose.dev.yml up -d --build` (the `--build` flag is critical)
     3. Remember, the container rebuild automatically includes building the Go binary

3. **Adding New Features**:
   - Backend changes: Add criteria in `internal/database/search/`
   - Update GraphQL schema in `graphql/schema/`
   - Frontend changes: Components in `webui/src/app/`
   - Update translations in `webui/src/app/i18n/translations/en.json`

## Code Style Guidelines
- **Go**: Standard Go conventions with proper error wrapping/handling
- **Imports**: stdlib first, external deps next, internal packages last
- **Package Structure**: Domain-driven packages with interfaces in root
- **Naming**: CamelCase for exported types, camelCase for variables
- **Testing**: Table-driven tests using testify/assert
- **Dependency Injection**: Uber FX with modules in `*fx/module.go`
- **WebUI**: Angular app with ESLint/Prettier for formatting
- **Error Handling**: Use error wrapping, custom error types with context

## Key Components
- **WebUI Integration**: UI is built with `npm run build` in the webui folder, which creates files in `webui/dist/`. These files are embedded in the Go binary during compilation via the `//go:embed` directive in `webui/embed.go`. This means:
  - WebUI changes require rebuilding both the Angular app AND the Go binary
  - Simply restarting the container won't pick up new WebUI changes
  - The complete rebuild flow is:
    1. Build WebUI: `cd webui && npm run build`
    2. Rebuild container: `docker-compose -f docker-compose.dev.yml up -d --build`
  - The container rebuild automatically includes building the Go binary with the embedded WebUI
  - Missing either of these steps will result in UI changes not appearing in the container
- **Database Models**: Generated code is in `internal/model/` with table names defined as constants (e.g., `TableNameTorrentContent`).
- **SQL Queries**: When referencing tables in SQL criteria, always use the correct table name (e.g., `torrent_contents.size` not `torrents.size`).
- **Search Filters**: Implemented as criteria in `internal/database/search/` and exposed through GraphQL in `internal/gql/`.
- **Angular Forms**: Form controls like `minSizeControl`, `maxSizeControl` bind to UI elements and manage state.
- **URL Parameters**: Handled in component subscriptions to `route.queryParams` and in `controlsToParams` function.
- **Component Structure**: Angular components split into HTML templates, TypeScript controllers, and SCSS styles.

## Common Patterns
- **Size Value Conversion**: When working with file sizes, convert between units (KB, MB, GB, TB) and bytes using appropriate multipliers (1024^n).
- **GraphQL Criteria**: Each filter needs corresponding criteria in both backend and frontend code.
- **Responsive Design**: Use Material components and Angular's responsive services like `breakpoints.sizeAtLeast('Medium')`.
- **Translations**: Always use translation keys (e.g., `t("torrents.size_filter")`) instead of hardcoded text.

## Development Tips
- **Testing UI Changes Locally**:
  - For faster UI development, you can run `cd webui && ng serve` to start a local development server
  - This allows seeing UI changes instantly without rebuilding the container
  - Note that GraphQL API calls will fail unless you proxy them to your running BitMagnet backend
  - Once UI changes are finalized, follow the complete build process for deployment
- **Container Optimization**:
  - Use `docker-compose -f docker-compose.dev.yml build --no-cache` when making significant changes
  - For debugging container issues: `docker exec -it bitmagnet-dev /bin/sh`

Adhere to existing patterns in each module when extending functionality.