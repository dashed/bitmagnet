//! Static Torznab category IDs and the category tree exposed by caps.

use crate::response::{Category, Subcategory};

pub const CATEGORY_MOVIES: i32 = 2000;
pub const CATEGORY_MOVIES_SD: i32 = 2030;
pub const CATEGORY_MOVIES_HD: i32 = 2040;
pub const CATEGORY_MOVIES_UHD: i32 = 2045;
pub const CATEGORY_MOVIES_3D: i32 = 2060;
pub const CATEGORY_AUDIO: i32 = 3000;
pub const CATEGORY_AUDIO_AUDIOBOOK: i32 = 3030;
pub const CATEGORY_PC: i32 = 4000;
pub const CATEGORY_PC_GAMES: i32 = 4050;
pub const CATEGORY_TV: i32 = 5000;
pub const CATEGORY_TV_SD: i32 = 5030;
pub const CATEGORY_TV_HD: i32 = 5040;
pub const CATEGORY_TV_UHD: i32 = 5045;
pub const CATEGORY_XXX: i32 = 6000;
pub const CATEGORY_XXX_OTHER: i32 = 6070;
pub const CATEGORY_BOOKS: i32 = 7000;
pub const CATEGORY_BOOKS_EBOOK: i32 = 7020;
pub const CATEGORY_BOOKS_COMICS: i32 = 7030;
pub const CATEGORY_OTHER: i32 = 8000;

/// Returns a fresh category value for a known Torznab category ID.
#[must_use]
pub fn category_by_id(id: i32) -> Option<Category> {
    match id {
        CATEGORY_MOVIES => Some(movies()),
        CATEGORY_MOVIES_SD => Some(leaf(CATEGORY_MOVIES_SD, "Movies/SD")),
        CATEGORY_MOVIES_HD => Some(leaf(CATEGORY_MOVIES_HD, "Movies/HD")),
        CATEGORY_MOVIES_UHD => Some(leaf(CATEGORY_MOVIES_UHD, "Movies/UHD")),
        CATEGORY_MOVIES_3D => Some(leaf(CATEGORY_MOVIES_3D, "Movies/3D")),
        CATEGORY_AUDIO => Some(audio()),
        CATEGORY_AUDIO_AUDIOBOOK => Some(leaf(CATEGORY_AUDIO_AUDIOBOOK, "Audio/Audiobook")),
        CATEGORY_PC => Some(pc()),
        CATEGORY_PC_GAMES => Some(leaf(CATEGORY_PC_GAMES, "PC/Games")),
        CATEGORY_TV => Some(tv()),
        CATEGORY_TV_SD => Some(leaf(CATEGORY_TV_SD, "TV/SD")),
        CATEGORY_TV_HD => Some(leaf(CATEGORY_TV_HD, "TV/HD")),
        CATEGORY_TV_UHD => Some(leaf(CATEGORY_TV_UHD, "TV/UHD")),
        CATEGORY_XXX => Some(xxx()),
        CATEGORY_XXX_OTHER => Some(leaf(CATEGORY_XXX_OTHER, "XXX/Other")),
        CATEGORY_BOOKS => Some(books()),
        CATEGORY_BOOKS_EBOOK => Some(leaf(CATEGORY_BOOKS_EBOOK, "Books/EBook")),
        CATEGORY_BOOKS_COMICS => Some(leaf(CATEGORY_BOOKS_COMICS, "Books/Comics")),
        CATEGORY_OTHER => Some(leaf(CATEGORY_OTHER, "Other")),
        _ => None,
    }
}

/// Returns the caps category tree in Go's `TopLevelCategories` order.
#[must_use]
pub fn top_level_categories() -> Vec<Category> {
    vec![movies(), audio(), pc(), tv(), xxx(), books(), other()]
}

fn category(id: i32, name: &str, subcat: Vec<Subcategory>) -> Category {
    Category {
        id,
        name: name.to_owned(),
        subcat,
    }
}

fn leaf(id: i32, name: &str) -> Category {
    category(id, name, Vec::new())
}

fn subcategory(id: i32, name: &str) -> Subcategory {
    Subcategory {
        id,
        name: name.to_owned(),
    }
}

fn movies() -> Category {
    category(
        CATEGORY_MOVIES,
        "Movies",
        vec![
            subcategory(CATEGORY_MOVIES_SD, "Movies/SD"),
            subcategory(CATEGORY_MOVIES_HD, "Movies/HD"),
            subcategory(CATEGORY_MOVIES_UHD, "Movies/UHD"),
            subcategory(CATEGORY_MOVIES_3D, "Movies/3D"),
        ],
    )
}

fn audio() -> Category {
    category(
        CATEGORY_AUDIO,
        "Audio",
        vec![subcategory(CATEGORY_AUDIO_AUDIOBOOK, "Audio/Audiobook")],
    )
}

fn pc() -> Category {
    category(
        CATEGORY_PC,
        "PC",
        vec![subcategory(CATEGORY_PC_GAMES, "PC/Games")],
    )
}

fn tv() -> Category {
    category(
        CATEGORY_TV,
        "TV",
        vec![
            subcategory(CATEGORY_TV_SD, "TV/SD"),
            subcategory(CATEGORY_TV_HD, "TV/HD"),
            subcategory(CATEGORY_TV_UHD, "TV/UHD"),
        ],
    )
}

fn xxx() -> Category {
    category(
        CATEGORY_XXX,
        "XXX",
        vec![subcategory(CATEGORY_XXX_OTHER, "XXX/Other")],
    )
}

fn books() -> Category {
    category(
        CATEGORY_BOOKS,
        "Books",
        vec![
            subcategory(CATEGORY_BOOKS_EBOOK, "Books/EBook"),
            subcategory(CATEGORY_BOOKS_COMICS, "Books/Comics"),
        ],
    )
}

fn other() -> Category {
    leaf(CATEGORY_OTHER, "Other")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn top_level_categories_match_the_go_order_and_shape() {
        let categories = top_level_categories();
        let actual = categories
            .iter()
            .map(|category| (category.id, category.name.as_str(), category.subcat.len()))
            .collect::<Vec<_>>();

        assert_eq!(
            actual,
            [
                (CATEGORY_MOVIES, "Movies", 4),
                (CATEGORY_AUDIO, "Audio", 1),
                (CATEGORY_PC, "PC", 1),
                (CATEGORY_TV, "TV", 3),
                (CATEGORY_XXX, "XXX", 1),
                (CATEGORY_BOOKS, "Books", 2),
                (CATEGORY_OTHER, "Other", 0),
            ]
        );
    }

    #[test]
    fn has_matches_the_category_or_any_direct_subcategory() {
        let movies = category_by_id(CATEGORY_MOVIES).expect("movies category exists");

        assert!(movies.has(CATEGORY_MOVIES));
        assert!(movies.has(CATEGORY_MOVIES_UHD));
        assert!(!movies.has(CATEGORY_TV));
    }

    #[test]
    fn named_leaf_categories_have_no_subcategories() {
        for id in [
            CATEGORY_MOVIES_SD,
            CATEGORY_MOVIES_HD,
            CATEGORY_MOVIES_UHD,
            CATEGORY_MOVIES_3D,
            CATEGORY_AUDIO_AUDIOBOOK,
            CATEGORY_PC_GAMES,
            CATEGORY_TV_SD,
            CATEGORY_TV_HD,
            CATEGORY_TV_UHD,
            CATEGORY_XXX_OTHER,
            CATEGORY_BOOKS_EBOOK,
            CATEGORY_BOOKS_COMICS,
            CATEGORY_OTHER,
        ] {
            let category = category_by_id(id).expect("named category exists");
            assert!(category.subcat.is_empty(), "category {id}");
        }
    }
}
