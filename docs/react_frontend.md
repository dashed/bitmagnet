# BitMagnet React Frontend Specification

A modern React-based frontend for BitMagnet, built from scratch with a focus on enhanced user experience, performance, and maintainability.

## Overview

- **Location**: `webui-react/` (parallel to existing `webui/`)
- **Deployment**: Embedded in Go binary via `go:embed` (same as Angular)
- **Coexistence**: Both frontends available indefinitely; user chooses

## Technology Stack

### Core

| Technology | Version | Purpose |
|------------|---------|---------|
| React | 18.x | UI framework |
| TypeScript | 5.x | Type safety |
| Vite | Latest | Build tool, dev server |
| pnpm | Latest | Package manager |

### UI Framework

| Technology | Version | Purpose |
|------------|---------|---------|
| Mantine | 8.x (latest ~8.3.12) | Component library |
| Tabler Icons | Latest | Icon set (Mantine default) |
| Mantine Transitions | Built-in | Animations |
| Mantine Notifications | Built-in | Toast notifications |

### Data & State

| Technology | Purpose |
|------------|---------|
| TanStack Query | Server state management, caching |
| graphql-request | GraphQL client |
| graphql-codegen | TypeScript type generation from schema |
| React Context | Simple global state (theme, preferences) |
| fuse.js | Client-side fuzzy search |

### Routing & Forms

| Technology | Purpose |
|------------|---------|
| TanStack Router | Type-safe routing |
| TanStack Form | Form handling and validation |
| TanStack Table | Headless table with Mantine styling |

### Utilities

| Technology | Purpose |
|------------|---------|
| date-fns | Date/time manipulation |
| react-i18next | Internationalization |
| cmdk | Command palette |
| Recharts | Dashboard charts |

### Development

| Technology | Purpose |
|------------|---------|
| Vitest | Unit testing |
| Testing Library | Component testing |
| ESLint (react-app config) | Linting |
| Prettier | Formatting |
| Husky | Pre-commit hooks |

## Project Structure

```
webui-react/
├── src/
│   ├── features/           # Feature-based modules
│   │   ├── search/         # Torrent search
│   │   ├── dashboard/      # Dashboard & monitoring
│   │   ├── settings/       # User preferences
│   │   └── queue/          # Queue management
│   ├── components/         # Shared UI components
│   ├── hooks/              # Shared React hooks
│   ├── lib/                # Utilities, GraphQL client
│   ├── i18n/               # Translations (fresh, not ported)
│   ├── routes/             # TanStack Router config
│   └── theme/              # Custom Mantine theme
├── tests/                  # Integration tests
├── public/                 # Static assets
├── Makefile                # Build commands
└── package.json
```

## URL Structure

| Route | Purpose |
|-------|---------|
| `/` | Redirect to `/search` |
| `/search` | Main torrent search with filters |
| `/search?q=...&filters=...` | Shareable search with URL state |
| `/dashboard` | System overview, health, metrics |
| `/dashboard/queues` | Queue monitoring and management |
| `/settings` | User preferences |

## Features

### Core Features (Parity with Angular)

1. **Torrent Search**
   - Full-text search (server-side via GraphQL)
   - Faceted filtering: content type, source, tags, file type, language, genre, resolution, video source
   - **Torrent size filter**: Range slider + preset buttons (<100MB, 100MB-1GB, 1GB-10GB, 10GB+)
   - **Published at filter**: Preset periods (Today, 7d, 30d, 90d, Year) + custom date range picker
   - Sorting by relevance, date, size, seeders, leechers, name
   - Traditional pagination with page size selector
   - URL-encoded filters for shareable searches

2. **Torrent Details**
   - Slide-over panel (keeps search visible)
   - Metadata display with poster images (direct TMDB URLs)
   - File listing (paginated for large torrents)
   - Tag management via dedicated modal
   - Reprocess with options dialog
   - Delete with simple confirmation
   - Copy magnet link + toast notification

3. **Dashboard**
   - Health status with persistent header indicator + alerts
   - Queue metrics and job listing
   - Torrent metrics with Recharts visualizations
   - Enhanced charts compared to Angular

4. **Queue Management**
   - Job listing and filtering
   - Purge completed jobs
   - Batch reprocess operations

### Enhanced Features (Beyond Angular)

1. **Keyboard Navigation**
   - Vim-style shortcuts: j/k (up/down), g (go to), / (search focus)
   - Help modal on `?` keypress

2. **Command Palette (Cmd+K)**
   - Quick navigation to pages
   - Recent searches
   - Quick actions (theme toggle, etc.)
   - Torrent quick search with fuzzy matching

3. **Saved Searches**
   - Save filter combinations to localStorage
   - Generate shareable URLs
   - Quick access from command palette

4. **Better Mobile Experience**
   - Responsive hybrid layout: sidebar on desktop, bottom nav on mobile
   - Touch-optimized interactions
   - Proper mobile breakpoints

5. **Bulk Operations**
   - Checkbox column + floating action bar
   - Shift+click range selection
   - Ctrl/Cmd+click multi-select (file manager style)
   - Batch tag, delete, reprocess

6. **Real-time Updates (via SSE)**
   - New torrents added notifications
   - Queue job status changes
   - Health status change alerts
   - *Requires backend SSE implementation*

7. **Fuzzy Search**
   - fuse.js for client-side fuzzy matching
   - Command palette search
   - Tag autocomplete
   - Instant refinement of search results

## UI/UX Design

### Theme

- **Approach**: Custom theme from scratch
- **Dark Mode**: System preference detection + manual toggle
- **Layout**: Responsive hybrid (sidebar desktop, bottom nav mobile)

### Search Interface

- **Query Input**: Submit on enter (explicit)
- **Facet Filters**: Instant apply (as you select)
- **Filter Display**: Inline chips + expandable panel for all options

### Loading States

- Skeleton loaders for content areas
- Progress bar for page transitions
- Shimmer effects on placeholders

### Error Handling

- Toast notifications for mutations (non-blocking)
- Inline error states for queries (with retry)
- Graceful degradation when offline (show cached data, disable mutations)

### Empty States

- Clear message + suggestions
- Clear filters button
- Popular search suggestions

### Accessibility

- Target: WCAG 2.1 AA compliance
- Semantic HTML
- Full keyboard navigation
- Screen reader support

## Data Handling

### GraphQL Integration

- Generated TypeScript types from schema via graphql-codegen
- TanStack Query for caching and state management
- Optimistic updates for mutations

### Offline Support

- Read-only cache when backend unreachable
- Show offline banner
- Disable mutations while offline

### Image Handling

- Direct TMDB URLs (no proxy)
- Browser caching (eager load)
- Fallback placeholder on failure

## Settings

### Included Settings

- Theme (dark/light/system)
- Language selection
- Table density
- Default page size
- Keyboard shortcut customization
- Notification preferences
- Tag colors (user-defined per tag)

### Storage

- Backend user settings API (PostgreSQL)
- Syncs across devices

## Backend Requirements

This spec requires the following backend additions:

### 1. SSE Endpoint

**Purpose**: Real-time event streaming

**Events**:
- `torrent:added` - New torrent indexed
- `queue:job:completed` - Background job finished
- `queue:job:failed` - Background job failed
- `health:changed` - Service health status change

**Implementation**: Research best approach during development (standard library or r3labs/sse)

### 2. User Settings API

**Purpose**: Persist user preferences

**Storage**: PostgreSQL (existing database)

**Endpoints**:
- `GET /api/settings` - Retrieve user settings
- `PUT /api/settings` - Update user settings

**Schema**: JSON blob or structured columns TBD

## Development Workflow

### Makefile Commands

```makefile
dev          # Start Vite dev server
build        # Production build
lint         # ESLint check
lint:fix     # ESLint auto-fix
format       # Prettier format
format:check # Prettier check
test         # Run Vitest
test:watch   # Vitest watch mode
codegen      # GraphQL type generation
typecheck    # TypeScript check
preview      # Preview production build
analyze      # Bundle size analysis
```

### Pre-commit Hooks

- lint-staged runs ESLint + Prettier on staged files
- TypeScript check on commit

### Testing Strategy

- Unit tests co-located with components (`*.test.ts`)
- Integration tests in `tests/` directory
- Vitest + Testing Library
- Focus on user interactions and accessibility

## Browser Support

- Modern browsers only (last 2 versions)
- Chrome, Firefox, Safari, Edge
- Full mobile browser support (responsive design)

## Phased Implementation

### Phase 1: Foundation

- Project setup (Vite, TypeScript, pnpm)
- Mantine theme configuration
- GraphQL client + codegen setup
- TanStack Router configuration
- App shell with responsive layout
- Basic authentication check (if backend has auth)

### Phase 2: Search

- Search page with query input
- Faceted filter implementation
- Size filter (slider + presets)
- Published at filter (presets + date picker)
- Results table with TanStack Table
- Pagination
- URL state management
- Slide-over details panel

### Phase 3: Torrent Management

- File listing (paginated)
- Tag management modal
- User-defined tag colors
- Reprocess dialog
- Delete confirmation
- Bulk selection and operations
- Copy magnet functionality

### Phase 4: Dashboard

- Health overview with indicators
- Queue job listing
- Queue metrics with Recharts
- Torrent metrics visualization
- Alert notifications for health changes

### Phase 5: Enhancements

- Keyboard navigation (vim-style)
- Command palette (cmdk)
- Saved searches
- Offline support (read-only cache)
- Settings page with all preferences

### Phase 6: Real-time & Backend

- Backend SSE implementation
- Frontend SSE client integration
- Backend user settings API
- Settings sync across devices

### Phase 7: Polish

- i18n implementation (fresh translations)
- Accessibility audit and fixes
- Performance optimization
- Documentation

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Backend API changes | Generate types from schema; version pin if needed |
| Design consistency | Define theme system early; use design principles skill |
| Bundle size growth | Manual analysis during development; tree-shaking |

## Acceptance Criteria

### Functional

- [ ] All Angular features replicated
- [ ] Enhanced features implemented (keyboard, cmd palette, saved searches)
- [ ] Size and date filters working
- [ ] Real-time updates via SSE
- [ ] Settings persist across devices
- [ ] Offline mode shows cached data

### Non-Functional

- [ ] WCAG 2.1 AA accessibility
- [ ] Responsive on mobile, tablet, desktop
- [ ] Page load < 3s on 3G
- [ ] Lighthouse score > 90

### Technical

- [ ] TypeScript strict mode
- [ ] ESLint + Prettier passing
- [ ] Test coverage on critical paths
- [ ] GraphQL types auto-generated

---

*Interview completed: 40+ questions asked*
*Spec refined: 2026-01-15*
