# Report – IMDb Big Data Processing

## 1. Data preparation

### Results

<!-- results:preparation:start -->

**Five columns with the highest share of missing values (%)**

| table | column | rows | missing_count | missing_pct |
|---|---|---|---|---|
| title.akas | attributes | 60,321,594 | 60,004,431 | 99.474 |
| title.basics | endYear | 12,951,113 | 12,786,793 | 98.731 |
| name.basics | deathYear | 15,802,420 | 15,537,057 | 98.321 |
| name.basics | birthYear | 15,802,420 | 15,116,831 | 95.662 |
| title.principals | job | 102,649,609 | 82,794,746 | 80.658 |

**Missing values per column (%)**

| table | column | null_count | empty_count | missing_pct |
|---|---|---|---|---|
| name.basics | deathYear | 15,537,057 | 0 | 98.321 |
| name.basics | birthYear | 15,116,831 | 0 | 95.662 |
| name.basics | primaryProfession | 3,216,642 | 0 | 20.355 |
| name.basics | knownForTitles | 1,915,685 | 0 | 12.123 |
| name.basics | primaryName | 89 | 0 | 0.001 |
| name.basics | nconst | 0 | 0 | 0 |
| title.akas | attributes | 60,004,431 | 0 | 99.474 |
| title.akas | types | 40,342,236 | 0 | 66.879 |
| title.akas | language | 19,639,034 | 0 | 32.557 |
| title.akas | region | 12,991,240 | 0 | 21.537 |
| title.akas | titleId | 0 | 0 | 0 |
| title.akas | ordering | 0 | 0 | 0 |
| title.akas | title | 0 | 0 | 0 |
| title.akas | isOriginalTitle | 0 | 0 | 0 |
| title.basics | endYear | 12,786,793 | 0 | 98.731 |
| title.basics | runtimeMinutes | 8,052,653 | 0 | 62.177 |
| title.basics | startYear | 1,487,768 | 0 | 11.488 |
| title.basics | genres | 547,430 | 0 | 4.227 |
| title.basics | tconst | 0 | 0 | 0 |
| title.basics | titleType | 0 | 0 | 0 |
| title.basics | primaryTitle | 0 | 0 | 0 |
| title.basics | originalTitle | 0 | 0 | 0 |
| title.basics | isAdult | 0 | 0 | 0 |
| title.crew | writers | 6,425,630 | 0 | 49.62 |
| title.crew | directors | 5,784,342 | 0 | 44.667 |
| title.crew | tconst | 0 | 0 | 0 |
| title.episode | seasonNumber | 2,120,177 | 0 | 21.149 |
| title.episode | episodeNumber | 2,120,177 | 0 | 21.149 |
| title.episode | tconst | 0 | 0 | 0 |
| title.episode | parentTconst | 0 | 0 | 0 |
| title.principals | job | 82,794,746 | 0 | 80.658 |
| title.principals | characters | 52,649,325 | 0 | 51.29 |
| title.principals | tconst | 0 | 0 | 0 |
| title.principals | ordering | 0 | 0 | 0 |
| title.principals | nconst | 0 | 0 | 0 |
| title.principals | category | 0 | 0 | 0 |
| title.ratings | tconst | 0 | 0 | 0 |
| title.ratings | averageRating | 0 | 0 | 0 |
| title.ratings | numVotes | 0 | 0 | 0 |

**Other data quality issues (checks with at least one affected row)**

| table | category | check | description | affected_rows | affected_pct |
|---|---|---|---|---|---|
| title.principals | cross_table | principal_died_before_title | principal died before the title's startYear (all categories) | 384,574 | 0.375 |
| title.principals | cross_table | principal_died_before_title_not_archive | same, excluding archive_footage / archive_sound | 287,308 | 0.28 |
| title.episode | cross_table | episode_before_series | episode startYear is earlier than its series startYear | 3,536 | 0.035 |
| title.principals | cross_table | principal_born_after_title | principal born after the title's startYear | 3,101 | 0.003 |
| title.basics | implausible_value | start_in_future | startYear is after 2026 (announced or wrong) | 6,017 | 0.046 |
| name.basics | implausible_value | probably_missing_death_year | no deathYear but born more than 110 years ago | 4,314 | 0.027 |
| title.basics | implausible_value | runtime_over_24h | runtimeMinutes is over 1440 (24 hours) | 209 | 0.002 |
| name.basics | implausible_value | lifespan_over_max | deathYear - birthYear is over 110 years | 39 | 0 |
| name.basics | impossible_value | death_before_birth | deathYear is earlier than birthYear | 28 | 0 |
| title.basics | impossible_value | non_positive_runtime | runtimeMinutes is 0 or negative | 4 | 0 |
| title.principals | orphan_foreign_key | orphan_nconst | rows in nconst with no match in name.basics.nconst | 8,441 | 0.008 |
| title.crew | orphan_foreign_key | orphan_writers | references in writers with no match in name.basics.nconst | 1,149 | 0.007 |
| title.crew | orphan_foreign_key | orphan_directors | references in directors with no match in name.basics.nconst | 423 | 0.004 |
| name.basics | orphan_foreign_key | orphan_knownForTitles | references in knownForTitles with no match in title.basics.tconst | 69 | 0 |
| title.crew | orphan_foreign_key | orphan_tconst | rows in tconst with no match in title.basics.tconst | 30 | 0 |
| title.episode | orphan_foreign_key | orphan_parentTconst | rows in parentTconst with no match in title.basics.tconst | 5 | 0 |
| title.akas | orphan_foreign_key | orphan_titleId | rows in titleId with no match in title.basics.tconst | 2 | 0 |

**Checks without any affected row**

| table | check | description |
|---|---|---|
| name.basics | birth_in_future | birthYear is after 2026 |
| name.basics | death_in_future | deathYear is after 2026 |
| name.basics | duplicate_key | extra rows sharing the same (nconst) |
| title.akas | duplicate_key | extra rows sharing the same (titleId, ordering) |
| title.akas | invalid_is_original_title | isOriginalTitle is not 0 or 1 |
| title.basics | duplicate_key | extra rows sharing the same (tconst) |
| title.basics | end_before_start | endYear is earlier than startYear |
| title.basics | end_year_on_non_series | endYear is set on a title that is not a series |
| title.basics | invalid_is_adult | isAdult is not 0 or 1 |
| title.basics | start_before_cinema | startYear is before 1874 |
| title.crew | duplicate_key | extra rows sharing the same (tconst) |
| title.episode | duplicate_key | extra rows sharing the same (tconst) |
| title.episode | episode_is_own_parent | parentTconst equals tconst |
| title.episode | episode_number_without_season | only one of seasonNumber / episodeNumber is set |
| title.episode | negative_season_or_episode | seasonNumber or episodeNumber is negative |
| title.episode | orphan_tconst | rows in tconst with no match in title.basics.tconst |
| title.principals | duplicate_key | extra rows sharing the same (tconst, ordering) |
| title.principals | orphan_tconst | rows in tconst with no match in title.basics.tconst |
| title.ratings | duplicate_key | extra rows sharing the same (tconst) |
| title.ratings | non_positive_votes | numVotes is 0 or negative |
| title.ratings | orphan_tconst | rows in tconst with no match in title.basics.tconst |
| title.ratings | rating_out_of_range | averageRating is outside [1, 10] |

**Cleaning summary (bronze -> silver)**

| table | rows_in | rows_out | dropped_rows | flagged_rows |
|---|---|---|---|---|
| title.basics | 12,951,113 | 12,951,113 | 0 | 6,230 |
| name.basics | 15,802,420 | 15,802,420 | 0 | 4,381 |
| title.akas | 60,321,594 | 60,321,592 | 2 | 0 |
| title.crew | 12,949,786 | 12,949,756 | 30 | 989 |
| title.ratings | 1,730,885 | 1,730,885 | 0 | 0 |
| title.episode | 10,025,088 | 10,025,083 | 5 | 3,536 |
| title.principals | 102,649,609 | 102,649,609 | 0 | 298,240 |

<!-- results:preparation:end -->

### Methodological explanation

**Missing Values Measurement:** We counted nulls and empty strings for every column directly on the "bronze" layer (the unmodified IMDb TSV files). Measuring before any cleaning is crucial to understanding the true, raw state of the source data. We tracked nulls and empty strings separately to distinguish between explicitly missing values and empty formatting anomalies, though parsing IMDb's `\N` marker as a true null resulted in zero empty strings across all tables.

**Interpreting High Null Rates:** The five columns with the highest missing rates are `title.akas.attributes` (99.5%), `title.basics.endYear` (98.7%), `name.basics.deathYear` (98.3%), `name.basics.birthYear` (95.7%), and `title.principals.job` (80.7%). However, a high null rate is not inherently a data quality defect. We must distinguish between "not applicable" and "unknown":
*   `endYear` only applies to TV series.
*   `deathYear` is naturally missing for living people.
*   `job` is intentionally left blank for acting categories.
*   **The Fame Factor:** Missing data often correlates with popularity. For instance, `birthYear` is populated for 50% of individuals with over 100 credits, but only 1.3% for those with zero credits.

**Analytical Challenges:** The truly problematic missing values for our downstream exploration were not the top five, but rather `runtimeMinutes` (62.2% missing, which directly impacts Q1) and `startYear` (11.5% missing).

**Quality Checks Categorization:** We implemented an exhaustive suite of data quality checks divided into 6 categories: impossible, implausible, cross-table inconsistencies, single-row inconsistencies, orphan foreign keys, and duplicates. Categorizing them is essential because it dictates our cleaning strategy: "impossible" data (like 28 people dying before birth) must be nullified, whereas "implausible" data might just be rare edge cases needing a flag. Notably, 22 of our checks returned exactly zero affected rows (including all duplicate checks), which is a positive result indicating strong structural integrity in certain areas.

**Threshold Justifications:** 
*   *110-year lifespan:* We set 110 years as the pragmatic upper bound for human life. Thus, anyone born over 110 years ago without a `deathYear` (4,314 rows) is flagged as implausibly still alive.
*   *First film in 1874:* This year marks the generally accepted dawn of motion pictures; anything dated earlier is an anomaly.
*   *24-hour runtime limit:* While some experimental art films exceed this, any standard title reporting a runtime over 1,440 minutes (209 rows) is highly suspicious and flagged.
*   *Archive footage distinction:* Our initial check for "principal died before the title's start year" flagged 384,574 rows. However, we created a refined version that explicitly excludes `archive_footage` and `archive_sound`. This dropped the anomaly count to 287,308. This distinction is crucial because posthumous appearances via archival material are perfectly valid and should not be treated as data errors.

**Data Cleaning (Bronze to Silver):** Our cleaning pipeline converted data types, transformed lists into arrays, and applied a strict logic for anomalies. Impossible values were nullified. Doubtful values were kept but tracked in a new `dq_flags` column (where an empty array signifies a clean row). We deleted exactly 37 rows that referenced non-existent titles (2 in `akas`, 30 in `crew`, 5 in `episode`), while rows referencing unknown people were kept and flagged. Unknown people were simply removed from the `directors` and `writers` arrays.

**Alternatives & Trade-offs:** We considered alternatives like deleting all flagged rows, keeping only one of two conflicting dates, or imputing missing data, but ultimately rejected them for the following reasons:
*   *Flagging instead of deleting:* We chose to flag implausible records because many are factually correct. For instance, "dead before film" cases mostly involve adapted authors (J.R.R. Tolkien for *The Lord of the Rings*, Bob Kane for *The Dark Knight*). "Born after film" records usually reflect restoration musicians (*Nosferatu*) or modern redubbings (*Fantasia*).
*   *Nullifying both dates:* When `birthYear` and `deathYear` logically contradict (28 cases of death before birth), we nullified both. Among these, 18 are ancient authors (B.C. dates stored without minus signs) and 10 are typos. It is impossible to programmatically know which date is correct.
*   *Handling orphans differently:* A movie database centers on titles. A row without a valid parent title is useless (hence the deletions), but a valid title with an unlinked actor is still highly valuable (hence kept and flagged).
*   *No imputation:* We deliberately chose *not* to impute missing `runtimeMinutes` (e.g., using the genre median). Doing so would artificially concentrate the data around the median, completely skewing our outlier detection methodology in Q1.

## 2. Data exploration

### Results

<!-- results:exploration:start -->

#### Q1 – Runtime outliers per genre

5,278 (genre, film) outliers, 3,730 distinct films.

**Runtime statistics and bounds per genre (minutes)**

| genre | films | avg_runtime | median_runtime | q1_min | q3_min | lower_bound_min | upper_bound_min | longer | shorter | outliers |
|---|---|---|---|---|---|---|---|---|---|---|
| Drama | 196,714 | 96.1 | 93 | 81 | 107 | 3 | 185 | 956 | 0 | 956 |
| Documentary | 116,146 | 78 | 74 | 57 | 90 | -42 | 189 | 748 | 0 | 748 |
| Comedy | 96,649 | 93.1 | 91 | 82 | 102 | 22 | 162 | 716 | 0 | 716 |
| Romance | 42,442 | 99.1 | 95 | 85 | 110 | 10 | 185 | 128 | 0 | 128 |
| Action | 40,375 | 101.2 | 94 | 85 | 113 | 1 | 197 | 309 | 0 | 309 |
| Crime | 34,643 | 95.5 | 92 | 83 | 105 | 17 | 171 | 284 | 0 | 284 |
| Thriller | 34,096 | 96.1 | 93 | 85 | 105 | 25 | 165 | 209 | 0 | 209 |
| Horror | 30,748 | 88.6 | 88 | 79 | 96 | 28 | 147 | 326 | 0 | 326 |
| Adventure | 24,063 | 93 | 90 | 79 | 101 | 13 | 167 | 436 | 0 | 436 |
| Mystery | 16,483 | 94.3 | 92 | 81 | 105 | 9 | 177 | 88 | 0 | 88 |
| Biography | 16,107 | 89.2 | 88 | 69 | 104 | -36 | 209 | 60 | 0 | 60 |
| Family | 15,947 | 92.3 | 90 | 77 | 102 | 2 | 177 | 89 | 0 | 89 |
| Music | 13,976 | 88.4 | 87 | 68 | 100 | -28 | 196 | 109 | 0 | 109 |
| History | 13,638 | 94.3 | 90 | 72 | 108 | -36 | 216 | 112 | 0 | 112 |
| Fantasy | 12,720 | 93.9 | 91 | 80 | 104 | 8 | 176 | 112 | 0 | 112 |
| Sci-Fi | 9,815 | 91.2 | 90 | 79 | 100 | 16 | 163 | 97 | 0 | 97 |
| Musical | 8,506 | 100 | 94 | 82 | 111 | -5 | 198 | 43 | 0 | 43 |
| Animation | 8,340 | 82.7 | 81 | 68 | 93 | -7 | 168 | 82 | 0 | 82 |
| War | 8,050 | 94.8 | 92 | 80 | 105 | 5 | 180 | 102 | 0 | 102 |
| Sport | 6,823 | 88.9 | 90 | 71 | 100 | -16 | 187 | 45 | 0 | 45 |
| Western | 6,378 | 76.5 | 70 | 57 | 90 | -42 | 189 | 76 | 0 | 76 |
| Adult | 6,123 | 79.5 | 77 | 66 | 85 | 9 | 142 | 125 | 0 | 125 |
| News | 1,408 | 77.6 | 75 | 60 | 88 | -24 | 172 | 19 | 0 | 19 |
| Film-Noir | 877 | 81.9 | 82 | 71 | 91 | 11 | 151 | 0 | 0 | 0 |
| Reality-TV | 298 | 92.2 | 90 | 64.3 | 96 | -31 | 191.3 | 7 | 0 | 7 |
| Talk-Show | 104 | 87.6 | 80 | 60 | 113.5 | -100.5 | 274 | 0 | 0 | 0 |

**Largest outliers (distance outside the fence, in IQRs)**

| genre | tconst | primaryTitle | startYear | runtimeMinutes | genre_median_runtime | iqr_distance | direction |
|---|---|---|---|---|---|---|---|
| Documentary | tt8273150 | Logistics | 2,012 | 51,420 | 74 | 1,555.45 | longer |
| Documentary | tt3854496 | Ambiancé | 2,020 | 43,200 | 74 | 1,306.36 | longer |
| Documentary | tt12277054 | Carnets Filmés (Liste Complète) | 2,019 | 28,643 | 74 | 865.24 | longer |
| Documentary | tt2659636 | Modern Times Forever | 2,011 | 14,400 | 74 | 433.64 | longer |
| Documentary | tt2355497 | Beijing 2003 | 2,004 | 9,000 | 74 | 270 | longer |
| Comedy | tt5136218 | London EC1 | 2,015 | 5,460 | 91 | 267.9 | longer |
| Drama | tt5068890 | Hunger! | 2,015 | 6,000 | 93 | 226.65 | longer |
| Mystery | tt5136218 | London EC1 | 2,015 | 5,460 | 92 | 223.13 | longer |
| Drama | tt1806963 | Matrjoschka | 2,006 | 5,700 | 93 | 215.12 | longer |
| Drama | tt5136218 | London EC1 | 2,015 | 5,460 | 93 | 205.88 | longer |
| Documentary | tt5068890 | Hunger! | 2,015 | 6,000 | 74 | 179.09 | longer |
| Music | tt0284020 | The Cure for Insomnia | 1,987 | 5,220 | 87 | 160 | longer |
| Documentary | tt0284020 | The Cure for Insomnia | 1,987 | 5,220 | 74 | 155.45 | longer |
| Drama | tt3837350 | A 2nd generation film | 2,013 | 3,077 | 93 | 114.23 | longer |
| Drama | tt0342707 | The Longest Most Meaningless Movie in the World | 1,970 | 2,880 | 93 | 106.65 | longer |
| Family | tt1866307 | World Peace & Prayer Day | 2,011 | 2,400 | 90 | 91.92 | longer |
| Documentary | tt5135246 | Saynatakuna: Masks | 2,013 | 2,905 | 74 | 85.3 | longer |
| Documentary | tt0342707 | The Longest Most Meaningless Movie in the World | 1,970 | 2,880 | 74 | 84.55 | longer |
| Horror | tt7521772 | 24 Hour Psycho | 1,993 | 1,440 | 88 | 79.06 | longer |
| Comedy | tt1885195 | Azgrab: The Documentary | 2,011 | 1,669 | 91 | 78.35 | longer |

#### Q2 – Top three genres per year

**Top 3 genres per year, for each popularity measure**

| year | total_votes | number_of_films | average_rating |
|---|---|---|---|
| 1,980 | Drama, Adventure, Horror | Drama, Comedy, Action | Biography, Romance, Music |
| 1,981 | Adventure, Drama, Horror | Drama, Comedy, Action | Music, Animation, History |
| 1,982 | Drama, Sci-Fi, Action | Drama, Comedy, Action | Animation, History, Drama |
| 1,983 | Drama, Action, Adventure | Drama, Comedy, Action | Biography, Drama, Mystery |
| 1,984 | Action, Drama, Adventure | Drama, Comedy, Action | Documentary, History, Drama |
| 1,985 | Comedy, Adventure, Drama | Drama, Comedy, Action | Biography, Animation, Drama |
| 1,986 | Drama, Adventure, Comedy | Drama, Comedy, Action | Animation, Family, Drama |
| 1,987 | Drama, Comedy, Action | Drama, Comedy, Action | Biography, War, Animation |
| 1,988 | Drama, Comedy, Action | Drama, Comedy, Action | Documentary, Animation, Biography |
| 1,989 | Comedy, Drama, Adventure | Drama, Comedy, Action | History, Animation, War |
| 1,990 | Drama, Comedy, Crime | Drama, Comedy, Action | Biography, History, War |
| 1,991 | Drama, Crime, Comedy | Drama, Comedy, Action | Biography, Animation, Drama |
| 1,992 | Drama, Comedy, Crime | Drama, Comedy, Action | History, Music, Biography |
| 1,993 | Drama, Comedy, Crime | Drama, Comedy, Action | History, Biography, Animation |
| 1,994 | Drama, Crime, Comedy | Drama, Comedy, Action | Biography, Animation, Drama |
| 1,995 | Drama, Crime, Comedy | Drama, Comedy, Action | Animation, History, Biography |
| 1,996 | Drama, Comedy, Crime | Drama, Comedy, Action | War, Music, Biography |
| 1,997 | Drama, Action, Comedy | Drama, Comedy, Romance | Documentary, Animation, Biography |
| 1,998 | Drama, Comedy, Crime | Drama, Comedy, Romance | War, Documentary, History |
| 1,999 | Drama, Comedy, Crime | Drama, Comedy, Romance | Documentary, Biography, History |
| 2,000 | Drama, Comedy, Adventure | Drama, Comedy, Romance | Documentary, War, History |
| 2,001 | Drama, Comedy, Adventure | Drama, Comedy, Romance | Documentary, Animation, History |
| 2,002 | Drama, Adventure, Action | Drama, Comedy, Documentary | Documentary, Biography, History |
| 2,003 | Drama, Action, Comedy | Drama, Comedy, Documentary | Documentary, History, Biography |
| 2,004 | Drama, Action, Comedy | Drama, Comedy, Documentary | Biography, Documentary, Sport |
| 2,005 | Drama, Comedy, Action | Drama, Comedy, Documentary | Documentary, Biography, War |
| 2,006 | Drama, Comedy, Thriller | Drama, Comedy, Documentary | Documentary, History, Biography |
| 2,007 | Drama, Comedy, Action | Drama, Comedy, Documentary | Documentary, Biography, War |
| 2,008 | Drama, Action, Comedy | Drama, Comedy, Documentary | Documentary, Biography, History |
| 2,009 | Drama, Comedy, Action | Drama, Documentary, Comedy | Documentary, Biography, History |
| 2,010 | Drama, Action, Adventure | Drama, Documentary, Comedy | Documentary, Biography, War |
| 2,011 | Drama, Action, Comedy | Drama, Documentary, Comedy | Documentary, Sport, Biography |
| 2,012 | Drama, Action, Comedy | Drama, Documentary, Comedy | Documentary, Biography, Music |
| 2,013 | Drama, Action, Comedy | Drama, Documentary, Comedy | Documentary, Biography, Sport |
| 2,014 | Drama, Action, Adventure | Drama, Documentary, Comedy | Documentary, Biography, History |
| 2,015 | Drama, Action, Adventure | Drama, Documentary, Comedy | Documentary, Sport, Biography |
| 2,016 | Drama, Action, Adventure | Drama, Documentary, Comedy | Documentary, Biography, History |
| 2,017 | Drama, Action, Comedy | Drama, Documentary, Comedy | Documentary, Biography, Sport |
| 2,018 | Drama, Action, Adventure | Drama, Documentary, Comedy | Documentary, Sport, Biography |
| 2,019 | Drama, Action, Comedy | Drama, Documentary, Comedy | Documentary, Music, Biography |
| 2,020 | Drama, Comedy, Action | Drama, Documentary, Comedy | Documentary, Biography, History |
| 2,021 | Drama, Action, Adventure | Drama, Documentary, Comedy | Documentary, Music, Biography |
| 2,022 | Drama, Action, Comedy | Drama, Documentary, Comedy | Documentary, Music, Sport |
| 2,023 | Drama, Action, Comedy | Drama, Documentary, Comedy | Music, Documentary, Sport |
| 2,024 | Drama, Action, Comedy | Drama, Documentary, Comedy | Documentary, Music, Biography |
| 2,025 | Drama, Action, Comedy | Drama, Documentary, Comedy | War, Documentary, Music |

**Years in the top 3 (popularity = votes)**

| genre | years_in_top | years_first | first_year | last_year |
|---|---|---|---|---|
| Drama | 46 | 42 | 1,980 | 2,025 |
| Comedy | 34 | 2 | 1,985 | 2,025 |
| Action | 29 | 1 | 1,982 | 2,025 |
| Adventure | 16 | 1 | 1,980 | 2,021 |
| Crime | 9 | 0 | 1,990 | 1,999 |
| Horror | 2 | 0 | 1,980 | 1,981 |
| Thriller | 1 | 0 | 2,006 | 2,006 |
| Sci-Fi | 1 | 0 | 1,982 | 1,982 |

**Years in the top 3 (popularity = films)**

| genre | years_in_top | years_first | first_year | last_year |
|---|---|---|---|---|
| Drama | 46 | 46 | 1,980 | 2,025 |
| Comedy | 46 | 0 | 1,980 | 2,025 |
| Documentary | 24 | 0 | 2,002 | 2,025 |
| Action | 17 | 0 | 1,980 | 1,996 |
| Romance | 5 | 0 | 1,997 | 2,001 |

**Years in the top 3 (popularity = avg_rating)**

| genre | years_in_top | years_first | first_year | last_year |
|---|---|---|---|---|
| Biography | 35 | 8 | 1,980 | 2,024 |
| Documentary | 31 | 27 | 1,984 | 2,025 |
| History | 20 | 3 | 1,981 | 2,020 |
| Animation | 13 | 3 | 1,981 | 2,001 |
| Music | 11 | 2 | 1,980 | 2,025 |
| War | 10 | 3 | 1,987 | 2,025 |
| Sport | 8 | 0 | 2,004 | 2,023 |
| Drama | 7 | 0 | 1,982 | 1,994 |
| Romance | 1 | 0 | 1,980 | 1,980 |
| Family | 1 | 0 | 1,986 | 1,986 |
| Mystery | 1 | 0 | 1,983 | 1,983 |

#### Q3 – Top three genres compared with the other genres

**Top 3 genres (by total votes) vs the other genres, per year**

| year | top_vote_share_pct | top_film_share_pct | top_votes_per_film | rest_votes_per_film | top_avg_rating | rest_avg_rating | other_genres |
|---|---|---|---|---|---|---|---|
| 1,980 | 44.3 | 37.0 | 5,699 | 4,207 | 6.08 | 6.25 | 21 |
| 1,981 | 46.5 | 35.7 | 3,884 | 2,484 | 6.22 | 6.52 | 20 |
| 1,982 | 43.7 | 38.6 | 5,032 | 4,072 | 6.12 | 6.36 | 19 |
| 1,983 | 45.9 | 41.0 | 4,327 | 3,558 | 6.05 | 6.16 | 19 |
| 1,984 | 44.2 | 39.4 | 6,199 | 5,094 | 6.24 | 6.32 | 19 |
| 1,985 | 51.5 | 46.8 | 5,973 | 4,943 | 6.36 | 6.21 | 19 |
| 1,986 | 46.2 | 47.3 | 5,018 | 5,233 | 6.38 | 6.26 | 19 |
| 1,987 | 42.6 | 52.9 | 4,437 | 6,701 | 6.14 | 6.25 | 19 |
| 1,988 | 46.8 | 51.4 | 5,180 | 6,225 | 6.23 | 6.32 | 19 |
| 1,989 | 49.5 | 45.7 | 5,821 | 4,998 | 6.31 | 6.24 | 19 |
| 1,990 | 45.6 | 50.3 | 6,441 | 7,771 | 6.35 | 6.21 | 19 |
| 1,991 | 44.2 | 50.3 | 5,973 | 7,638 | 6.45 | 6.18 | 19 |
| 1,992 | 46.3 | 50.5 | 6,921 | 8,183 | 6.53 | 6.35 | 19 |
| 1,993 | 44.8 | 49.8 | 8,499 | 10,398 | 6.37 | 6.36 | 19 |
| 1,994 | 58.3 | 50.9 | 13,573 | 10,079 | 6.36 | 6.13 | 19 |
| 1,995 | 49 | 48.4 | 12,983 | 12,710 | 6.43 | 6.22 | 19 |
| 1,996 | 44.9 | 49.3 | 8,278 | 9,876 | 6.4 | 6.27 | 20 |
| 1,997 | 44.1 | 52.1 | 10,776 | 14,862 | 6.27 | 6.44 | 19 |
| 1,998 | 51.5 | 50.9 | 11,932 | 11,646 | 6.45 | 6.35 | 19 |
| 1,999 | 43.6 | 51.2 | 12,272 | 16,609 | 6.45 | 6.4 | 19 |
| 2,000 | 45 | 46.0 | 11,646 | 12,142 | 6.28 | 6.31 | 19 |
| 2,001 | 42.4 | 46.7 | 12,492 | 14,886 | 6.21 | 6.35 | 19 |
| 2,002 | 42.4 | 37.9 | 15,332 | 12,689 | 6.16 | 6.38 | 22 |
| 2,003 | 45.5 | 50.6 | 11,490 | 14,101 | 6.27 | 6.31 | 21 |
| 2,004 | 41.3 | 48.8 | 11,909 | 16,112 | 6.25 | 6.41 | 21 |
| 2,005 | 40.4 | 48.0 | 9,317 | 12,706 | 6.12 | 6.31 | 21 |
| 2,006 | 42.7 | 47.1 | 10,559 | 12,640 | 6.05 | 6.23 | 20 |
| 2,007 | 38.3 | 45.0 | 9,939 | 13,103 | 6.12 | 6.28 | 21 |
| 2,008 | 40.4 | 45.3 | 9,485 | 11,598 | 6.08 | 6.21 | 20 |
| 2,009 | 41.3 | 44.8 | 8,566 | 9,869 | 6.03 | 6.2 | 20 |
| 2,010 | 40 | 33.0 | 10,751 | 7,946 | 6.18 | 6.25 | 21 |
| 2,011 | 46.1 | 42.7 | 9,050 | 7,897 | 6.08 | 6.2 | 21 |
| 2,012 | 42.7 | 41.8 | 7,958 | 7,651 | 6 | 6.17 | 21 |
| 2,013 | 41.5 | 41.1 | 8,319 | 8,168 | 6.01 | 6.23 | 21 |
| 2,014 | 47 | 30.8 | 12,005 | 6,017 | 6.03 | 6.2 | 23 |
| 2,015 | 46.6 | 31.8 | 9,267 | 4,944 | 6.02 | 6.2 | 21 |
| 2,016 | 44.1 | 32.1 | 9,156 | 5,484 | 6.02 | 6.17 | 21 |
| 2,017 | 45.1 | 41.7 | 6,693 | 5,851 | 6.01 | 6.15 | 22 |
| 2,018 | 44.3 | 32.5 | 8,045 | 4,869 | 6.03 | 6.2 | 22 |
| 2,019 | 43.6 | 41.9 | 6,275 | 5,841 | 5.97 | 6.17 | 22 |
| 2,020 | 44.7 | 39.0 | 4,246 | 3,352 | 5.84 | 6.02 | 22 |
| 2,021 | 46.4 | 32.3 | 7,572 | 4,160 | 5.99 | 6.12 | 22 |
| 2,022 | 46.4 | 41.7 | 5,353 | 4,419 | 6 | 6.22 | 22 |
| 2,023 | 44.8 | 42.9 | 4,499 | 4,173 | 6.05 | 6.27 | 22 |
| 2,024 | 42.9 | 41.3 | 3,777 | 3,540 | 6.11 | 6.19 | 23 |
| 2,025 | 39.3 | 41.4 | 3,406 | 3,714 | 6.1 | 6.27 | 22 |

#### Q4 – Directors with a single hit

| primaryName | hit_title | hit_year | hit_rating | hit_votes | other_movies | other_avg_rating | other_best_rating |
|---|---|---|---|---|---|---|---|
| Richard Kelly | Donnie Darko | 2,001 | 8 | 925,161 | 2 | 5.45 | 5.6 |
| Garth Davis | Lion | 2,016 | 8 | 270,263 | 2 | 5.65 | 5.9 |
| J.P. Dutta | Border | 1,997 | 8 | 20,775 | 4 | 5.33 | 5.5 |
| Karthick Naren | Dhuruvangal Pathinaaru | 2,016 | 8.1 | 19,410 | 2 | 5.15 | 5.9 |
| Ravikanth Perepu | Kshanam | 2,016 | 8.1 | 6,708 | 2 | 5.95 | 6 |
| Prince | Sign 'o' the Times | 1,987 | 8 | 3,054 | 2 | 4.9 | 5.1 |
| David Hogan | Sign 'o' the Times | 1,987 | 8 | 3,054 | 2 | 4.6 | 5.6 |
| Marcus Raboy | Conan O'Brien: The Kennedy Center Mark Twain Prize for American Humor | 2,025 | 8 | 1,584 | 2 | 5.45 | 5.8 |

#### Q5 – Series whose worst episode is in the last season

133 series.

**Series (30 first, by best episode rating)**

| series_title | series_start | seasons | rated_episodes | best_seasonNumber | best_episode_title | best_averageRating | worst_seasonNumber | worst_episode_title | worst_averageRating |
|---|---|---|---|---|---|---|---|---|---|
| Demon Slayer: Kimetsu no Yaiba | 2,019 | 4 | 63 | 2 | Never Give Up | 9.8 | 4 | To Bring a Smile to One's Face | 7.1 |
| To Your Eternity | 2,021 | 3 | 62 | 1 | Awakening | 9.7 | 3 | Stay Here | 6.6 |
| Young Justice | 2,010 | 4 | 98 | 2 | Summit | 9.7 | 4 | Nautical Twilight | 6.8 |
| We Bare Bears | 2,014 | 4 | 138 | 2 | Yuri and the Bear | 9.6 | 4 | Ranger Norm | 6.3 |
| The Flash | 2,014 | 9 | 184 | 1 | Out of Time | 9.6 | 9 | The Good, the Bad and the Lucky | 3.8 |
| House of Cards | 2,013 | 6 | 73 | 2 | Chapter 14 | 9.5 | 6 | Chapter 73 | 2.5 |
| One Punch Man | 2,015 | 3 | 36 | 1 | The Strongest Hero | 9.5 | 3 | Motley Heroes | 1.6 |
| Chuck | 2,007 | 5 | 91 | 2 | Chuck Versus the Ring | 9.5 | 5 | Chuck Versus the Bearded Bandit | 7.4 |
| As If | 2,021 | 6 | 68 | 2 | Çaça ve Cosplay | 9.4 | 6 | Girls Night Out | 6.2 |
| Mythic Quest | 2,020 | 4 | 40 | 1 | A Dark Quiet Death | 9.4 | 4 | Rebrand | 6.5 |
| Narcos | 2,015 | 3 | 30 | 2 | Al Fin Cayó! | 9.4 | 3 | The Kingpin Strategy | 7.8 |
| For My Son | 2,015 | 3 | 82 | 1 | Episode #1.23 | 9.4 | 3 | Episode #3.18 | 6.6 |
| Dynasty | 2,017 | 5 | 108 | 1 | Dead Scratch | 9.3 | 5 | A Writer of Dubious Talent | 6.8 |
| Silicon Valley | 2,014 | 6 | 53 | 1 | Optimal Tip-To-Tip Efficiency | 9.3 | 6 | Maximizing Alphaness | 7.3 |
| Primal | 2,019 | 3 | 30 | 2 | The Red Mist | 9.3 | 3 | Kingdom of Sorrow | 7.8 |
| Danny Phantom | 2,003 | 3 | 49 | 2 | The Ultimate Enemy | 9.3 | 3 | Livin' Large | 6.7 |
| Wu-Tang: An American Saga | 2,019 | 3 | 30 | 2 | Protect Ya Neck | 9.3 | 3 | Dirty Dancin' | 5.5 |
| The Boondocks | 2,005 | 4 | 56 | 2 | Stinkmeaner Strikes Back | 9.3 | 4 | Granddad Dates a Kardashian | 5.8 |
| Ben 10: Ultimate Alien | 2,010 | 3 | 52 | 1 | The Forge of Creation | 9.3 | 3 | The Mother of All Vreedles | 5.1 |
| 24 | 2,001 | 8 | 192 | 1 | 11:00 p.m.-12:00 a.m. | 9.3 | 8 | Day 8: 12:00 a.m.-1:00 a.m. | 7.6 |
| Talking Tom & Friends | 2,014 | 5 | 65 | 1 | A Secret Worth Keeping: Part 3 | 9.2 | 5 | Tom the Guru | 5.5 |
| Star Trek | 1,966 | 3 | 80 | 1 | The City on the Edge of Forever | 9.2 | 3 | And the Children Shall Lead | 5.2 |
| The Man in the High Castle | 2,015 | 4 | 40 | 2 | Fallout | 9.2 | 4 | Fire from the Gods | 6 |
| Station 19 | 2,018 | 7 | 105 | 2 | Always Ready | 9.2 | 7 | True Colors | 6.5 |
| Ben 10: Alien Force | 2,008 | 3 | 46 | 2 | War of the Worlds: Pt. 2 | 9.2 | 3 | Fool's Gold | 6.2 |
| Undercover | 2,011 | 6 | 40 | 2 | Episode #2.12 | 9.2 | 6 | Episode #6.3 | 4.6 |
| The Resident | 2,018 | 6 | 107 | 2 | If Not Now, When? | 9.1 | 6 | Peek and Shriek | 6.9 |
| iZombie | 2,015 | 5 | 71 | 2 | Salivation Army | 9.1 | 5 | Dot Zom | 7.3 |
| 13 Reasons Why | 2,017 | 4 | 49 | 1 | Tape 7, Side A | 9.1 | 4 | Senior Camping Trip | 5.1 |
| Atypical | 2,017 | 4 | 38 | 2 | Ernest Shackleton's Rules for Survival | 9.1 | 4 | Magical Bird #1 | 7.4 |

#### Q6 – Actors successful in several languages

**Wikidata original language of the successful films**

| status | count |
|---|---|
| ok | 1,532 |
| no_item | 175 |
| no_language | 158 |

313 people with successful films in at least 2 languages, 40 in at least 3 languages.

**Number of people per number of languages**

| languages | count |
|---|---|
| 2 | 273 |
| 3 | 35 |
| 4 | 4 |
| 5 | 1 |

**People with successful films in at least 3 languages**

| primaryName | languages | language_list | successful_films |
|---|---|---|---|
| Amrish Puri | 5 | English, Hindustani, Malayalam, Tamil, Telugu | 7 |
| Jayaram | 4 | Kannada, Malayalam, Tamil, Telugu | 7 |
| Ashish Vidyarthi | 4 | Hindustani, Malayalam, Tamil, Telugu | 6 |
| Mukesh Rishi | 4 | Hindustani, Marathi, Tamil, Telugu | 4 |
| Silk Smitha | 4 | Hindustani, Malayalam, Tamil, Telugu | 4 |
| Nassar | 3 | Hindustani, Tamil, Telugu | 25 |
| Kamal Haasan | 3 | Hindustani, Tamil, Telugu | 23 |
| Prakash Raj | 3 | Kannada, Tamil, Telugu | 19 |
| Mammootty | 3 | English, Malayalam, Tamil | 15 |
| Achyuth Kumar | 3 | Kannada, Tamil, Telugu | 12 |
| Kishore Kadam | 3 | Hindustani, Marathi, Tamil | 10 |
| Madhavan | 3 | English, Hindustani, Tamil | 9 |
| Max von Sydow | 3 | English, French, Swedish | 9 |
| Tabu | 3 | Hindustani, Malayalam, Tamil | 9 |
| Saurabh Shukla | 3 | English, Hindustani, Tamil | 8 |
| Paresh Rawal | 3 | Hindustani, Tamil, Telugu | 8 |
| Revathi | 3 | Malayalam, Tamil, Telugu | 8 |
| V. Jayaprakash | 3 | Malayalam, Tamil, Telugu | 7 |
| Mohammad Ali | 3 | Kannada, Tamil, Telugu | 6 |
| Mrinal Kulkarni | 3 | English, Hindustani, Marathi | 6 |
| Sridevi | 3 | Hindustani, Tamil, Telugu | 6 |
| Geetha | 3 | Malayalam, Tamil, Telugu | 5 |
| Sachin Khedekar | 3 | Marathi, Tamil, Telugu | 5 |
| Prakash Belawadi | 3 | Hindustani, Kannada, Malayalam | 5 |
| Sonali Kulkarni | 3 | English, Hindustani, Marathi | 5 |
| Shobana | 3 | Malayalam, Tamil, Telugu | 5 |
| Tinnu Anand | 3 | Malayalam, Tamil, Telugu | 5 |
| Rohini | 3 | Malayalam, Tamil, Telugu | 5 |
| Siddharth | 3 | Hindustani, Tamil, Telugu | 4 |
| Predrag Lakovic | 3 | Romani, Romanian, Serbo-Croatian | 4 |
| Rao Ramesh | 3 | Kannada, Tamil, Telugu | 4 |
| Esther Anil | 3 | Malayalam, Tamil, Telugu | 4 |
| Dana Abed | 3 | Arabic, English, Telugu | 4 |
| Rajan P. Dev | 3 | Malayalam, Tamil, Telugu | 4 |
| Rohini Hattangadi | 3 | English, Hindustani, Tamil | 4 |
| Jisshu Sengupta | 3 | Bangla, Hindustani, Telugu | 3 |
| Sonali Bendre | 3 | Hindustani, Tamil, Telugu | 3 |
| Aruna Irani | 3 | Gujarati, Hindustani, Tamil | 3 |
| Atul Kulkarni | 3 | Hindustani, Kannada, Marathi | 3 |
| Sai Pallavi | 3 | Hindustani, Malayalam, Tamil | 3 |

#### Q7 – Long careers that peaked early

4,665 people.

**Largest drops (25 first)**

| primaryName | first_year | last_year | movies | rated_movies | best_title | best_year | best_rating | early_avg_rating | late_avg_rating | rating_drop |
|---|---|---|---|---|---|---|---|---|---|---|
| Tatyana Dogileva | 1,981 | 2,026 | 58 | 5 | A Railway Station for Two | 1,983 | 7.8 | 7.57 | 3.65 | 3.92 |
| Grant Williams | 1,956 | 1,976 | 16 | 8 | The Incredible Shrinking Man | 1,957 | 7.6 | 6.52 | 2.9 | 3.62 |
| Wendell Corey | 1,947 | 1,968 | 39 | 22 | Rear Window | 1,954 | 8.4 | 7.01 | 3.65 | 3.36 |
| Ralf Richter | 1,984 | 2,024 | 30 | 9 | Bang Boom Bang - Ein todsicheres Ding | 1,999 | 7.5 | 6.7 | 3.45 | 3.25 |
| Zeenat Aman | 1,970 | 2,025 | 66 | 10 | Don | 1,978 | 7.7 | 6.99 | 3.8 | 3.19 |
| Reza Naji | 1,997 | 2,025 | 32 | 5 | Children of Heaven | 1,997 | 8.2 | 7.8 | 4.7 | 3.1 |
| Nicolas Coster | 1,964 | 2,020 | 23 | 11 | Reds | 1,981 | 7.3 | 6.42 | 3.4 | 3.02 |
| Irina Rozanova | 1,986 | 2,026 | 68 | 5 | The Rifleman of the Voroshilov Regiment | 1,999 | 7.4 | 7.2 | 4.2 | 3 |
| Sphadikam George | 1,993 | 2,026 | 58 | 9 | Spadikam | 1,995 | 8.6 | 7.49 | 4.5 | 2.99 |
| Toyah Willcox | 1,979 | 2,024 | 19 | 5 | Quadrophenia | 1,979 | 7.2 | 6.75 | 3.77 | 2.98 |
| Akbar Abdi | 1,987 | 2,023 | 65 | 12 | Mother | 1,990 | 8.4 | 7.28 | 4.43 | 2.85 |
| Cüneyt Arkın | 1,964 | 2,014 | 263 | 18 | The Mine | 1,978 | 7.5 | 6.34 | 3.5 | 2.84 |
| Evelyn Keyes | 1,938 | 1,989 | 38 | 17 | Gone with the Wind | 1,939 | 8.2 | 6.97 | 4.15 | 2.82 |
| Caroline Munro | 1,972 | 2,023 | 23 | 13 | The Spy Who Loved Me | 1,977 | 7 | 5.62 | 2.85 | 2.77 |
| Fedi Nuril | 2,004 | 2,026 | 26 | 5 | The Heaven None Missed | 2,015 | 7.4 | 7.2 | 4.45 | 2.75 |
| Helen | 1,953 | 2,012 | 174 | 9 | Jewel Thief | 1,967 | 7.9 | 7.28 | 4.55 | 2.73 |
| Ennio Girolami | 1,952 | 2,002 | 62 | 8 | Nights of Cabiria | 1,957 | 8.1 | 7.4 | 4.67 | 2.73 |
| Madhan Bob | 1,985 | 2,026 | 68 | 16 | Thevar Magan | 1,992 | 8.6 | 7.52 | 4.83 | 2.69 |
| J.D. Chakravarthi | 1,989 | 2,026 | 42 | 10 | Satya | 1,998 | 8.2 | 7.6 | 4.91 | 2.69 |
| Prashant Narayanan | 2,003 | 2,026 | 30 | 7 | Shadows of Time | 2,004 | 7.6 | 6.72 | 4.05 | 2.67 |
| James Seay | 1,940 | 1,961 | 51 | 6 | The Face Behind the Mask | 1,941 | 7.1 | 6.83 | 4.17 | 2.67 |
| Mykel Shannon Jenkins | 2,004 | 2,026 | 22 | 7 | Undisputed III: Redemption | 2,010 | 7.3 | 6.6 | 3.94 | 2.66 |
| Robert Shayne | 1,934 | 1,975 | 65 | 12 | Mr. Skeffington | 1,944 | 7.5 | 6.86 | 4.2 | 2.66 |
| Edward Earle | 1,916 | 1,951 | 73 | 5 | The Wind | 1,928 | 8 | 7.1 | 4.45 | 2.65 |
| Firat Tanis | 2,002 | 2,024 | 28 | 9 | Once Upon a Time in Anatolia | 2,011 | 7.8 | 6.55 | 3.92 | 2.63 |

#### Q8 – Multi-talented people

**Multi-talented people per role families**

| roles | count |
|---|---|
| directing+writing | 54,032 |
| acting+directing+writing | 32,358 |
| acting+writing | 21,705 |
| acting+directing | 7,855 |

**Average rating per combination of roles held on the same film**

| combo | person_movies | people | avg_rating | median_rating | avg_votes |
|---|---|---|---|---|---|
| acting | 76,827 | 15,570 | 6.19 | 6.3 | 40,526 |
| writing | 26,617 | 11,328 | 6.24 | 6.4 | 41,224 |
| directing+writing | 25,796 | 12,183 | 6.28 | 6.5 | 24,607 |
| directing | 17,015 | 6,817 | 6.24 | 6.4 | 31,568 |
| acting+writing | 3,320 | 2,285 | 5.95 | 6.1 | 25,501 |
| acting+directing+writing | 2,444 | 1,505 | 6.02 | 6.2 | 17,049 |
| acting+directing | 693 | 514 | 6.03 | 6.3 | 27,196 |

**Multi-role vs single-role films, per person**

| people | avg_multi_role | avg_single_role | avg_difference | pct_better_multi_role |
|---|---|---|---|---|
| 8,694 | 6.09 | 6.09 | 0 | 50.9 |

<!-- results:exploration:end -->

### Methodological explanation

#### Q1 – Runtime outliers per genre

**Methodology & Thresholds:** We defined a "feature film" strictly as a movie with a runtime $> 40$ minutes, aligning with the Oscars, AFI, and BFI definitions. Films with multiple genres were counted in each of their respective genres. We excluded genres with fewer than 30 films and those where the Interquartile Range ($IQR$) was exactly $0$. To detect outliers, we applied Tukey’s IQR method using a strict multiplier of $k=3$. An anomaly is any film shorter than $Q1 - 3 \times IQR$ or longer than $Q3 + 3 \times IQR$. 

**Alternatives & Trade-offs:**
*   *IQR vs. Z-score:* We initially tested the Z-score method but rejected it because it is heavily skewed by extreme outliers. For instance, the 857-hour documentary *Logistics* inflated the standard deviation for documentaries to 223 minutes, masking other anomalies and detecting only 31 films. The IQR relies on quartiles, making it completely resistant to such extremes. Consequently, our IQR bounds remained identical whether movies $> 24$ hours were kept in the dataset or not.
*   *Multiplier $k=3$ vs $k=1.5$:* The standard Tukey multiplier is $1.5$, but this flags "mild" outliers (movies that are just slightly long). We deliberately used $k=3$ to isolate only "extreme" outliers.
*   *Zero "too short" outliers:* Our results returned 5,278 anomalous (genre, film) pairs spanning 3,730 distinct films, but not a single one was flagged as "too short." This is a mathematical certainty: the calculated lower bounds for all genres fall at 28 minutes or less (some are even negative). Since our baseline dataset strictly excludes films $\le 40$ minutes, falling below the lower bound is impossible.

#### Q2 – Top three genres per year

**Methodology & Metrics:** The term "popular" is inherently ambiguous. To address this, we extracted the top 3 genres per year from 1980 to 2025 using three distinct interpretations: total votes, number of films produced, and average rating. For the average rating metric, we enforced a strict threshold: only films with $\ge 1,000$ votes were included, and only genres with $\ge 10$ such films were evaluated.

**Alternatives & Trade-offs:**
*   *Three metrics vs. One:* We calculated all three because popularity can imply mass engagement (votes), industry supply (film count), or critical acclaim (rating). The results diverge drastically depending on the metric: "Drama" ranks 1st in votes for 42 out of 46 years, whereas "Biography", "Documentary", and "History" completely dominate the average rating metric.
*   *The Best Metric:* We conclude that **total votes** is the truest measure of actual popularity. It directly reflects audience reach and active mass engagement. In contrast, the number of films only reflects studio production volume, and average ratings heavily favor niche critical prestige over mainstream popularity. 
*   *Threshold constraints:* The minimum vote and film requirements for the rating metric were vital to prevent a genre from artificially winning a year just because it had one obscure, highly-rated masterpiece.

**Limitations:** Vote counts represent cumulative totals captured today rather than at the time of release, creating an inherent temporal bias. Furthermore, films with multiple genres are counted in every applicable category, and the year 2026 was completely excluded because it is an incomplete year.

#### Q3 – Top three genres compared with the other genres

**Methodology & Metrics:** To compare the Top 3 genres (defined by total votes) against all other genres each year, we utilized four specific indicators. *Vote share* measures their overall market weight; *film share* measures production volume; *votes per film* measures audience intensity and engagement per release; and *average rating* measures perceived quality.

**Results & Interpretation:** The data reveals a fascinating paradox. While the Top 3 genres command massive attention (gathering roughly 45% of all votes annually), they actually receive *lower* average ratings than the remaining genres in 34 out of the 46 years analyzed. We interpret this as the "mainstream penalty": highly popular genres (like Action or Comedy) produce a high volume of mass-market films that attract broad, casual audiences, leading to average or mixed reviews. In contrast, the "other" category contains niche genres (like Film-Noir or Documentary) whose viewers are already self-selected, dedicated fans, naturally resulting in higher baseline ratings.

**Limitations:** A distinct limitation of this analysis is circularity. Because the "Top 3" was explicitly defined by total votes in Q2, their overwhelming dominance in the "vote share" indicator is a mathematical certainty rather than an independent discovery.

#### Q4 – Directors with a single hit

**Methodology & Thresholds:** We filtered the dataset for feature films (>40 mins) with a minimum of 1,000 votes. This vote threshold was deliberately chosen to avoid rating biases caused by small, unrepresentative sample sizes. Directors were extracted from the `title.crew` table.

**Alternatives & Trade-offs:** To qualify, a director needed exactly one hit (rating $\ge 8.0$) and at least two other films. We considered interpreting the prompt by calculating the *overall average* of their other movies ($\le 6.0$). However, this alternative was discarded because it could allow a director to have a moderately good movie (e.g., rated 7.0) hidden within a low average. To strictly isolate true "one-hit wonders" with exclusively poor subsequent/prior films, we required *every single one* of their other movies to be rated $\le 6.0$.

**Results:** This strict approach successfully narrowed the dataset down to exactly 8 directors, highlighting cases like Richard Kelly (*Donnie Darko*) and Garth Davis (*Lion*).

#### Q5 – Series whose worst episode is in the last season

**Methodology & Thresholds:** We analyzed `tvSeries` and `tvMiniSeries`, defining the "last season" as the one with the highest season number (ignoring Season 0 and unknown seasons). To ensure statistical relevance, we required episodes to have at least 100 votes, and the series to have at least 10 rated episodes overall. We deliberately chose a 100-vote threshold for episodes rather than the 1,000 used for movies because user votes are heavily diluted across the 10 million episodes in the dataset. The median episode receives only 22 votes (compared to 78 for a movie). Without this threshold, obscure series skewed the results with unrepresentative 1.0 or 10.0 ratings from a handful of users.

**Alternatives & Trade-offs:** We interpreted "at least three seasons consisting of $\ge 10$ episodes" to mean that *any* three seasons needed to meet the 10-episode mark, rather than enforcing it on every single season. For rating ties, we applied a strict rule to respect the prompt's spirit: if multiple episodes tied for the lowest score, *all* of them had to be in the last season. Similarly, *all* episodes tied for the highest score had to be in seasons 1 or 2.

**Results:** This rigorous filtering successfully identified 133 series.

#### Q6 – Actors successful in several languages

**Methodology & Data Enrichment:** We defined a successful film as a movie rated $\ge 7.0$ with at least 1,000 votes. Because IMDb does not provide a film's original language, we enriched the dataset by politely querying Wikidata (property P364), an open (CC0), human-edited knowledge base. We achieved an 82% coverage rate (1,532 out of 1,865 films) and extracted the *first* language listed for each title.

**Alternatives & Trade-offs:** Initially, we tried to deduce the original language from localized titles in the `title.akas` table. This method proved highly inaccurate: for example, Toshirô Mifune was incorrectly credited for Swedish and Dutch films simply because those countries retained the romanized title *Rashômon*. Consequently, we switched to Wikidata. Furthermore, we deliberately grouped mutually intelligible languages (e.g., Serbian, Croatian, and Bosnian into Serbo-Croatian; American English into English). While grouping Hindi and Urdu into Hindustani is debatable since they use different alphabets, we prioritized spoken mutual intelligibility. In contrast, Cantonese and Mandarin were kept separate.

**Limitations:** By strictly selecting only the first listed language on Wikidata, multilingual films can be misclassified (e.g., *The Deer Hunter* is listed as "Russian" first). Additionally, 18% of the target films were dropped due to missing Wikidata items or language properties.

**Results:** We identified 313 people with successful films in at least two distinct languages, and 40 individuals in three or more.

#### Q7 – Long careers that peaked early

**Methodology & Thresholds:** We defined a long career as spanning $\ge 20$ years and including $\ge 15$ feature films. To accurately capture an actor's true active years, this career span was measured across *all* their movies. However, to evaluate their "peak," we only compared ratings on reliable films ($\ge 1,000$ votes). We defined an "early peak" as meeting two conditions: the actor's absolute best-rated film must fall in the first half of their career, and their average rating in the first half must be strictly higher than in the second half.

**Alternatives & Trade-offs:** 
*   *Two different bases:* We deliberately separated career measurement from performance evaluation. An actor's career length should reflect all their work, but their success should only be judged on widely watched movies.
*   *Minimum films per period:* We required at least 5 reliable films in total, with a minimum of 2 in each half of the career. Without this rule, the data was too volatile. For example, Jerzy Zelnik was flagged based on just one rated film per half (dropping drastically from a 7.3 to a 1.2).
*   *Rating vs. Votes:* We measured peak success using average ratings rather than total votes, because vote counts heavily bias toward recent films and would unfairly penalize older "early" peaks.
*   *Defining "Early":* We chose to divide the career into halves rather than thirds. Using thirds would shrink the comparison windows too much, making it difficult for actors to meet the minimum threshold of rated films per period.

**Results:** This methodology identified 4,665 people who fit the "early peak" profile.

#### Q8 – Multi-talented people

**Methodology & Definitions:** We categorized film professionals into three core role families: acting, directing, and writing (extracted from `title.principals` and `title.crew`). To be classified as "multi-talented," an individual had to perform in at least two of these role families across *different* films. Merely holding multiple roles on a single film was not sufficient to earn this classification. 

**Alternatives & Trade-offs:** 
*   *Role selection:* We restricted the analysis to acting, directing, and writing because they represent the primary creative and storytelling pillars of filmmaking. Roles like producers or composers were excluded, as they often reflect financial, administrative, or highly specialized external contributions rather than direct on-set creative leadership.
*   *Different films requirement:* We required the roles to be held on *different* projects to prove true, sustained versatility. A person who vanity-directs a single project they star in shouldn't automatically be broadly classified as "multi-talented" if they never direct again.
*   *Dual performance metrics:* We evaluated performance using two distinct measures: the global average rating for specific role combinations on a single film, and an intra-personal comparison (how a person's multi-role films compare to their single-role films). This dual approach is necessary to distinguish between general industry trends and individual effectiveness.

**Results:** We identified over 116,000 multi-talented people, with directing+writing being the most common combination. Interestingly, on an individual level, juggling roles makes zero difference: the average rating per person is exactly 6.09 for both multi-role and single-role films (with 50.9% doing slightly better in multi-role). However, globally, combinations that include acting (e.g., acting+writing at 5.95) tend to receive slightly lower ratings.

## 3. Data enrichment

### Results

<!-- results:enrichment:start -->

**Coverage**

| item | targets | enriched | enriched_pct |
|---|---|---|---|
| films | 6,258 | 5,320 | 85 |
| film principals | 18,773 | 15,370 | 81.9 |

**Notes for the films without a synopsis**

| synopsis_note | count |
|---|---|
|  | 5,320 |
| The Wikipedia article has no plot or synopsis section. | 497 |
| No English Wikipedia article is linked to this IMDb id in Wikidata. | 441 |

**Notes for the principals without a biography**

| biography_note | count |
|---|---|
|  | 15,370 |
| No English Wikipedia article is linked to this IMDb id in Wikidata. | 3,402 |
| The Wikipedia article has no text for this field. | 1 |

**Sample of enriched films**

| tconst | primaryTitle | startYear | synopsis_words | synopsis_source_url |
|---|---|---|---|---|
| tt0111161 | The Shawshank Redemption | 1,994 | 658 | https://en.wikipedia.org/wiki/The_Shawshank_Redemption |
| tt0468569 | The Dark Knight | 2,008 | 633 | https://en.wikipedia.org/wiki/The_Dark_Knight |
| tt1375666 | Inception | 2,010 | 678 | https://en.wikipedia.org/wiki/Inception |
| tt0137523 | Fight Club | 1,999 | 541 | https://en.wikipedia.org/wiki/Fight_Club |
| tt0816692 | Interstellar | 2,014 | 700 | https://en.wikipedia.org/wiki/Interstellar_%28film%29 |

<!-- results:enrichment:end -->

### Methodological explanation

**Sources & Linkage:** We used Wikipedia (CC BY-SA) for text enrichment and Wikidata (CC0) for ID mapping. We chose these over APIs like TMDb or OMDb to guarantee open-licensed, strictly human-authored content. To link IMDb IDs to Wikipedia articles, we queried Wikidata's P345 property rather than performing text-based title searches. Title searches are highly prone to errors due to remakes, homonyms, and translated titles. The accepted trade-off is that 441 films lacking a Wikidata item were skipped.

**Target Selection:** For films (1980-2025), we extracted the Top 10 per genre and year based on *average rating* rather than total votes, prioritizing critical quality over mainstream reach. We enforced a minimum of 1,000 votes to ensure representative ratings, meaning some genre/year pairs naturally returned fewer than 10 films. For principals, we selected the top 3 overall credits (`ordering`) rather than filtering only for actors, ensuring we capture essential creators like directors and writers.

**API Respect & Provenance:** We strictly adhered to Wikimedia's API policies: 1 request per second, a custom User-Agent detailing the project and repository, and `maxlag=5` to respect server loads. We implemented exponential backoffs (respecting `Retry-After` headers) to handle the 429 Too Many Requests, 502 Bad Gateway, and network cutoff errors we actively encountered. To minimize loads, SPARQL requests were batched. Complete lineage is guaranteed via the `synopsis_source_url`, `biography_source_url`, `wikidata_url`, and `retrieved_at` columns.

**Spark Engine & Scalability:** Downloading was handled outside of Spark using a resumable Python script to separate network I/O bottlenecks from distributed processing. Spark exclusively handled the heavy lifting: schema enforcement, extracting specific sections ("Plot", "Synopsis", etc.), text cleaning, and joins. The pipeline is structurally completely scalable; only the target selection limits its scope. To scale this to the entire IMDb dataset, the only barrier is time (due to the 1 req/sec limit), which would require switching from the API to Wikipedia XML dumps.

**Missing Data & Alternatives:** We achieved high coverage: 85% for film synopses and 81.9% for biographies. Instead of leaving missing data as blank NULLs, we added programmatic notes to explicitly explain the absence (e.g., 441 films without articles, 497 articles without a plot section, 3,402 persons without articles). We actively discarded several alternatives during development: falling back to title searches, using the Wikipedia REST API, using article introductions when synopses were missing, ranking targets by votes, or expanding the fetch limit beyond 3 people.

## 4. Streaming data processing

### Results

<!-- results:streaming:start -->

**Relevant changes per entity type**

| entity_type | changes | pages | bot_changes | avg_delay_s | max_delay_s |
|---|---|---|---|---|---|
| person | 17 | 11 | 1 | 7.6 | 19 |
| movie | 5 | 4 | 0 | 4.8 | 11 |

**Most changed IMDb entities**

| imdb_id | imdb_name | entity_type | count |
|---|---|---|---|
| nm0004794 | Glen Campbell | person | 5 |
| nm0000071 | James Stewart | person | 3 |
| tt1881010 | Milana | movie | 2 |
| nm0240381 | Hilary Duff | person | 1 |
| nm0006168 | John Lennon | person | 1 |
| tt0441773 | Kung Fu Panda | movie | 1 |
| nm0002127 | Mariska Hargitay | person | 1 |
| nm10899733 | Naslen | person | 1 |
| nm0079273 | Paul Bettany | person | 1 |
| nm0436302 | Pawan Kalyan | person | 1 |
| nm10399505 | Rachel Zegler | person | 1 |
| nm1258970 | Russell Brand | person | 1 |
| tt0106417 | Sailor Moon R: The Movie: The Promise of the Rose | movie | 1 |
| nm0005078 | Stacy Keach | person | 1 |
| tt1262426 | Wicked | movie | 1 |

**5-minute windows: counts written by the stream (watermark) and batch counts at the end**

| window_start | window_end | entity_type | changes_batch_at_end | changes_written_by_stream |
|---|---|---|---|---|
| 2026-11-11 16:25:00 | 2026-11-11 16:30:00 | movie | 1 | 1 |
| 2026-11-11 16:25:00 | 2026-11-11 16:30:00 | person | 3 | 3 |
| 2026-11-11 16:30:00 | 2026-11-11 16:35:00 | person | 2 | 2 |
| 2026-11-11 16:35:00 | 2026-11-11 16:40:00 | movie | 1 | 1 |
| 2026-11-11 16:35:00 | 2026-11-11 16:40:00 | person | 3 | 3 |
| 2026-11-11 16:40:00 | 2026-11-11 16:45:00 | movie | 1 | 1 |
| 2026-11-11 16:40:00 | 2026-11-11 16:45:00 | person | 2 | 2 |
| 2026-11-11 16:45:00 | 2026-11-11 16:50:00 | movie | 2 |  |
| 2026-11-11 16:45:00 | 2026-11-11 16:50:00 | person | 5 |  |
| 2026-11-11 16:50:00 | 2026-11-11 16:55:00 | person | 2 |  |

**Streaming metrics per query**

| name | micro_batches | input_rows | avg_input_rows_per_s | avg_processed_rows_per_s | median_batch_ms | max_batch_ms |
|---|---|---|---|---|---|---|
| matches | 53 | 5,418 | 3.4 | 35.3 | 2,693 | 44,790 |
| windows | 52 | 5,482 | 3.4 | 22.3 | 4,421.5 | 46,871 |

<!-- results:streaming:end -->

### Methodological explanation

**Architecture:** The producer script (`wiki_producer.py`) captures the live Wikimedia `recentchange` feed, filters for the English Wikipedia (`enwiki`), and writes unmodified events into the `wikipedia-changes` Kafka topic. Spark Structured Streaming then reads from this topic, filters for relevance, computes windowed aggregations, and writes the Parquet output. We chose Kafka over directly reading the SSE feed in Spark for several reasons: it decouples ingestion from processing, allows resuming from failures via offsets and checkpoints, enables message replay, and Spark lacks a native source connector for SSE streams.

**Relevance Filtering:** The `lookup` step pre-generates a list of target Wikipedia articles corresponding to our enriched films and persons. The stream joins this broadcasted list against the incoming edits by matching page titles (ignoring case and underscores). Over a 25-minute test, we read 5,418 events and identified 22 relevant changes (17 persons, 5 movies, with Glen Campbell modified 5 times). 
*   *Alternatives & Trade-offs:* Our initial iteration used a broader filter (matching popular movie titles, names, and film categories). While it captured 134 matches, precision was unacceptably low due to homonyms (e.g., an edit to a "Wildwood" geographic page matching the film title). We explicitly sacrificed higher match volume for exact precision by strictly joining on pre-validated target titles.

**Late Data & Watermarking:** This is a critical challenge. We tracked two clocks: `event_time` (when the edit occurred on Wikipedia) and `kafka_time` (when it arrived in our topic), with ingestion delays averaging 4.8 to 7.6s, maxing at 19s. We aggregated counts into 5-minute tumbling windows based on `event_time`, implementing a 5-minute watermark.
*   *Watermark Trade-offs:* We set the watermark to 5 minutes—far beyond the measured 19s delay—to safely accommodate potential network spikes. The trade-off is latency: a longer watermark accepts much later data but forces Spark to hold state in memory longer and delays the final window emission.
*   *Output Mode:* We used `append` mode because we only want to write a window once it is finalized (when the watermark has safely passed its end time).
*   *The Streaming Limit:* The report tables show a fundamental streaming limitation: because the watermark only advances when new data arrives, the final windows (from 16:45 onwards) were never written by the active stream (missing 9 of the 22 matches). To solve this, a final batch count (`windows_final`) recalculates all windows from the raw `matches` output at shutdown, ensuring zero data loss. Raw matches are preserved regardless of delay.

**Streaming Metrics & Robustness:** Our query runs a micro-batch every 30 seconds, processing an average of 3.4 rows per second. The system easily keeps pace: median batch duration is only 2.7s for `matches` and 4.4s for `windows`. The isolated maximum spikes (45-47s) are artifacts of the very first micro-batch initializing the Spark session and loading the broadcast `lookup` data. 
To guarantee robustness, we implemented several mechanisms: the `--fresh` flag deletes checkpoints and outputs simultaneously for a clean restart, failing queries are automatically detected, the producer enforces its shutdown time even if the external stream hangs, and it accurately counts messages successfully delivered to Kafka. 

**Alternatives discarded:** We considered monitoring other wikis besides English, using 2-minute or 30-minute watermarks, utilizing sliding windows or `foreachBatch`, outputting only to the console, or completely bypassing Kafka.

## 5. Open question

### Question and motivation

**The Question:** Is the cinema industry truly a "small world"? Who is its actual center, and does Kevin Bacon really deserve his legendary reputation? Furthermore, how does this central figure evolve decade by decade?

**Motivation & Background:** I first studied Stanley Milgram's "Six Degrees of Separation" and the "Six Degrees of Kevin Bacon" game in high school. It is a great pleasure to finally demonstrate and verify this famous myth using real big data in this course. Instead of merely replicating the classic overall study—whose result is already well-known—I chose to analyze the graph by decade. This novel approach allows us to confidently identify which actor was truly the most prominent and "in vogue" during any specific era.

**Data Used:** We exclusively used the provided IMDb dataset to answer this question. Absolutely no external or additional data was required.

**Complexity:** This question is highly non-trivial to solve at scale. The graph encompasses 184,609 actors and approximately 1.95 million edges, requiring complex iterative Breadth-First Search (BFS) graph traversals and computationally heavy self-joins on the `title.principals` table.

### Results

<!-- results:open-question:start -->

**Co-star graph per period (giant_component_pct is out of all the credited actors)**

| graph | actors | connected_actors | edges | giant_component | giant_component_pct | avg_separation | median_separation | max_distance_seen | pct_pairs_within_6 |
|---|---|---|---|---|---|---|---|---|---|
| all years | 184,609 | 184,397 | 1,950,777 | 177,572 | 96.19 | 4.558 | 4 | 12 | 97.14 |
| 1980-1989 | 19,224 | 19,217 | 143,270 | 17,627 | 91.69 | 4.954 | 5 | 17 | 86.2 |
| 1990-1999 | 22,420 | 22,411 | 182,725 | 20,573 | 91.76 | 4.819 | 5 | 13 | 88.03 |
| 2000-2009 | 42,633 | 42,594 | 344,222 | 39,902 | 93.59 | 4.853 | 5 | 11 | 91.66 |
| 2010-2019 | 70,444 | 70,341 | 570,255 | 66,377 | 94.23 | 4.878 | 5 | 14 | 93.66 |
| 2020-2025 | 52,225 | 52,161 | 357,374 | 48,082 | 92.07 | 5.612 | 6 | 14 | 75.33 |

**Centre of each graph and rank of Kevin Bacon (among the exact candidates, and among the giant component on the estimate)**

| graph | centre_name | centre_avg_distance | centre_degree | candidates | bacon_avg_distance | bacon_rank_among_candidates | bacon_estimated_rank | giant_component |
|---|---|---|---|---|---|---|---|---|
| all years | Willem Dafoe | 3.023 | 798 | 18 | 3.262 | 16 | 545 | 177,572 |
| 1980-1989 | Harry Dean Stanton | 3.344 | 149 | 17 | 3.732 | 15 | 542 | 17,627 |
| 1990-1999 | Samuel L. Jackson | 3.163 | 239 | 19 | 3.354 | 19 | 86 | 20,573 |
| 2000-2009 | Willem Dafoe | 3.216 | 243 | 18 | 3.564 | 13 | 599 | 39,902 |
| 2010-2019 | Ben Kingsley | 3.315 | 235 | 20 | 3.702 | 18 | 1,040 | 66,377 |
| 2020-2025 | Ana de Armas | 3.876 | 79 | 21 | 4.509 | 12 | 1,291 | 48,082 |

**Exact average distance of the candidate centres**

| graph | primaryName | degree | avg_distance | eccentricity | estimated | top_degree |
|---|---|---|---|---|---|---|
| all years | Willem Dafoe | 798 | 3.023 | 8 | yes | yes |
| all years | Robert De Niro | 771 | 3.044 | 8 | yes | yes |
| all years | Samuel L. Jackson | 826 | 3.05 | 8 | no | yes |
| all years | Ben Kingsley | 602 | 3.057 | 8 | yes | no |
| all years | Michael Caine | 858 | 3.063 | 8 | no | yes |
| all years | Harvey Keitel | 720 | 3.066 | 8 | yes | no |
| all years | Liam Neeson | 654 | 3.09 | 8 | yes | no |
| all years | Bruce Willis | 764 | 3.093 | 8 | no | yes |
| all years | Nicolas Cage | 868 | 3.094 | 8 | yes | yes |
| all years | Gary Oldman | 500 | 3.125 | 8 | yes | no |
| all years | Woody Harrelson | 596 | 3.141 | 8 | yes | no |
| all years | Gabriel Byrne | 480 | 3.144 | 8 | yes | no |
| all years | William Hurt | 493 | 3.165 | 8 | yes | no |
| all years | Gérard Depardieu | 771 | 3.193 | 8 | no | yes |
| all years | Christopher Lee | 767 | 3.196 | 8 | no | yes |
| all years | Kevin Bacon | 451 | 3.262 | 8 | no | no |
| all years | Anupam Kher | 741 | 3.408 | 8 | no | yes |
| all years | Nassar | 786 | 3.802 | 8 | no | yes |
| 1980-1989 | Harry Dean Stanton | 149 | 3.344 | 11 | yes | yes |
| 1980-1989 | M. Emmet Walsh | 182 | 3.364 | 11 | yes | yes |
| 1980-1989 | Robert Loggia | 158 | 3.376 | 10 | no | yes |
| 1980-1989 | Kenneth McMillan | 158 | 3.384 | 11 | no | yes |
| 1980-1989 | Burt Reynolds | 158 | 3.395 | 11 | no | yes |
| 1980-1989 | Brion James | 135 | 3.399 | 11 | yes | no |
| 1980-1989 | Donald Pleasence | 191 | 3.402 | 11 | yes | yes |
| 1980-1989 | Sean Connery | 107 | 3.411 | 11 | yes | no |
| 1980-1989 | Ronald Lacey | 79 | 3.431 | 11 | yes | no |
| 1980-1989 | Gene Hackman | 148 | 3.433 | 11 | yes | no |
| 1980-1989 | Rutger Hauer | 112 | 3.435 | 11 | yes | no |
| 1980-1989 | Michael Caine | 184 | 3.466 | 11 | yes | yes |
| 1980-1989 | Denholm Elliott | 143 | 3.466 | 11 | yes | no |
| 1980-1989 | John Candy | 149 | 3.509 | 11 | no | yes |
| 1980-1989 | Kevin Bacon | 78 | 3.732 | 12 | no | no |
| 1980-1989 | Gérard Depardieu | 154 | 3.854 | 10 | no | yes |
| 1980-1989 | Kemal Sunal | 218 | 5.479 | 13 | no | yes |
| 1990-1999 | Samuel L. Jackson | 239 | 3.163 | 8 | yes | yes |
| 1990-1999 | Harvey Keitel | 232 | 3.186 | 8 | no | yes |
| 1990-1999 | Robert De Niro | 191 | 3.19 | 8 | yes | yes |
| 1990-1999 | Anthony Hopkins | 186 | 3.195 | 8 | yes | no |
| 1990-1999 | John Malkovich | 156 | 3.2 | 8 | yes | no |
| 1990-1999 | Christopher McDonald | 227 | 3.208 | 8 | no | yes |
| 1990-1999 | Kevin Pollak | 178 | 3.226 | 8 | yes | no |
| 1990-1999 | Bruce Willis | 216 | 3.228 | 8 | no | yes |
| 1990-1999 | Bill Paxton | 193 | 3.231 | 8 | no | yes |
| 1990-1999 | Gary Oldman | 129 | 3.234 | 7 | yes | no |
| 1990-1999 | Patricia Arquette | 150 | 3.259 | 8 | yes | no |
| 1990-1999 | Viggo Mortensen | 160 | 3.262 | 8 | yes | no |
| 1990-1999 | Kevin Spacey | 151 | 3.269 | 8 | yes | no |
| 1990-1999 | Brad Dourif | 187 | 3.271 | 8 | no | yes |
| 1990-1999 | Robin Williams | 195 | 3.272 | 8 | no | yes |
| 1990-1999 | Julianne Moore | 189 | 3.275 | 8 | no | yes |
| 1990-1999 | Whoopi Goldberg | 190 | 3.281 | 8 | no | yes |
| 1990-1999 | Fred Ward | 111 | 3.306 | 8 | yes | no |
| 1990-1999 | Kevin Bacon | 141 | 3.354 | 8 | no | no |
| 2000-2009 | Willem Dafoe | 243 | 3.216 | 7 | yes | yes |
| 2000-2009 | Brian Cox | 224 | 3.239 | 7 | yes | yes |
| 2000-2009 | Steve Buscemi | 224 | 3.261 | 7 | yes | yes |
| 2000-2009 | Thomas Kretschmann | 175 | 3.275 | 7 | yes | no |
| 2000-2009 | Ben Kingsley | 177 | 3.292 | 8 | yes | no |
| 2000-2009 | Woody Harrelson | 193 | 3.295 | 8 | yes | no |
| 2000-2009 | Angelina Jolie | 144 | 3.299 | 7 | yes | no |
| 2000-2009 | Ciarán Hinds | 190 | 3.304 | 7 | yes | no |
| 2000-2009 | Samuel L. Jackson | 266 | 3.319 | 7 | no | yes |
| 2000-2009 | Tom Wilkinson | 187 | 3.323 | 7 | yes | no |
| 2000-2009 | Gérard Depardieu | 267 | 3.333 | 7 | no | yes |
| 2000-2009 | Penélope Cruz | 193 | 3.352 | 7 | yes | no |
| 2000-2009 | Kevin Bacon | 105 | 3.564 | 8 | no | no |
| 2000-2009 | Om Puri | 232 | 3.656 | 8 | no | yes |
| 2000-2009 | Anupam Kher | 233 | 3.813 | 8 | no | yes |
| 2000-2009 | Paresh Rawal | 231 | 4.177 | 8 | no | yes |
| 2000-2009 | Amitabh Bachchan | 259 | 4.222 | 9 | no | yes |
| 2000-2009 | Prakash Raj | 234 | 4.321 | 8 | no | yes |
| 2010-2019 | Ben Kingsley | 235 | 3.315 | 11 | yes | no |
| 2010-2019 | Willem Dafoe | 231 | 3.332 | 10 | yes | no |
| 2010-2019 | Samuel L. Jackson | 267 | 3.34 | 10 | yes | yes |
| 2010-2019 | Liam Neeson | 208 | 3.351 | 10 | yes | no |
| 2010-2019 | Antonio Banderas | 226 | 3.355 | 10 | yes | no |
| 2010-2019 | Robert De Niro | 203 | 3.375 | 10 | yes | no |
| 2010-2019 | J.K. Simmons | 267 | 3.39 | 10 | no | yes |
| 2010-2019 | Nicolas Cage | 317 | 3.399 | 11 | no | yes |
| 2010-2019 | Toby Jones | 195 | 3.401 | 10 | yes | no |
| 2010-2019 | James Franco | 317 | 3.412 | 11 | no | yes |
| 2010-2019 | Gary Oldman | 182 | 3.417 | 11 | yes | no |
| 2010-2019 | Idris Elba | 193 | 3.432 | 11 | yes | no |
| 2010-2019 | Peter Stormare | 279 | 3.441 | 10 | no | yes |
| 2010-2019 | Danny Glover | 297 | 3.464 | 11 | no | yes |
| 2010-2019 | Danny Trejo | 326 | 3.466 | 10 | no | yes |
| 2010-2019 | Vera Farmiga | 174 | 3.477 | 11 | yes | no |
| 2010-2019 | Anupam Kher | 306 | 3.612 | 10 | no | yes |
| 2010-2019 | Kevin Bacon | 72 | 3.702 | 11 | no | no |
| 2010-2019 | Nassar | 396 | 4.184 | 11 | no | yes |
| 2010-2019 | Prakash Raj | 387 | 4.214 | 12 | no | yes |
| 2020-2025 | Ana de Armas | 79 | 3.876 | 9 | yes | no |
| 2020-2025 | Guy Pearce | 133 | 3.889 | 9 | yes | no |
| 2020-2025 | Chris Evans | 71 | 3.916 | 9 | yes | no |
| 2020-2025 | Antonio Banderas | 106 | 3.938 | 8 | yes | no |
| 2020-2025 | Willem Dafoe | 159 | 3.941 | 9 | yes | no |
| 2020-2025 | Anya Taylor-Joy | 99 | 3.972 | 9 | yes | no |
| 2020-2025 | Timothy Spall | 89 | 3.983 | 9 | yes | no |
| 2020-2025 | Olivia Colman | 113 | 3.99 | 9 | yes | no |
| 2020-2025 | Joe Alwyn | 53 | 4.09 | 10 | yes | no |
| 2020-2025 | Alessandro Nivola | 63 | 4.094 | 9 | yes | no |
| 2020-2025 | Nassar | 290 | 4.458 | 10 | no | yes |
| 2020-2025 | Kevin Bacon | 59 | 4.509 | 9 | no | no |
| 2020-2025 | Tanikella Bharani | 322 | 4.606 | 10 | no | yes |
| 2020-2025 | Yogi Babu | 334 | 4.636 | 10 | no | yes |
| 2020-2025 | Murli Sharma | 237 | 4.684 | 10 | no | yes |
| 2020-2025 | Sunil | 258 | 4.76 | 11 | no | yes |
| 2020-2025 | Vennela Kishore | 298 | 4.781 | 11 | no | yes |
| 2020-2025 | Ajay | 260 | 4.83 | 11 | no | yes |
| 2020-2025 | Shine Tom Chacko | 242 | 4.844 | 11 | no | yes |
| 2020-2025 | Srikanth Iyengar | 254 | 4.872 | 11 | no | yes |
| 2020-2025 | Siddique | 237 | 5.057 | 11 | no | yes |

**Number of actors at each distance (all years)**

| distance | Kevin Bacon | Willem Dafoe |
|---|---|---|
| 0 | 1 | 1 |
| 1 | 451 | 798 |
| 2 | 23,407 | 38,106 |
| 3 | 91,967 | 99,441 |
| 4 | 53,555 | 35,113 |
| 5 | 7,434 | 3,736 |
| 6 | 640 | 312 |
| 7 | 92 | 56 |
| 8 | 25 | 9 |

**Example chains from Kevin Bacon**

| distance | path |
|---|---|
| 2 | Kevin Bacon -[National Lampoon's Animal House (1978)]- John Belushi -[1941 (1979)]- Toshirō Mifune |
| 2 | Kevin Bacon -[Black Mass (2015)]- Johnny Depp -[Once Upon a Time in Mexico (2003)]- Willem Dafoe |
| 2 | Kevin Bacon -[Black Mass (2015)]- Joel Edgerton -[The Great Gatsby (2013)]- Amitabh Bachchan |

<!-- results:open-question:end -->

### Methodological explanation

**Graph Construction & Algorithm:** We built an undirected graph where vertices are actors and edges represent co-starring in a feature film ($\ge 1,000$ votes). Distance is measured strictly in steps, not the number of shared films. We implemented a Breadth-First Search (BFS) using PySpark DataFrames: at each step, the current frontier is joined with the edges and anti-joined against visited actors. Crucially, each BFS level was materialized to disk as Parquet files; otherwise, Spark's logical execution plan would grow exponentially and crash. Multiple starting nodes were processed concurrently.

**Sampling & Trade-offs:** 
*   *Pair limitations:* Computing the exact distance for all ~10 billion pairs is computationally unfeasible. We estimated average distances using 100 deterministically sampled actors (seed 42), explicitly excluding the 0-distance to themselves.
*   *Exact candidates:* We ran the exact BFS for the top 10 estimated centers, the 10 highest-degree actors (as a safety net against sampling bias), and Kevin Bacon. This explains Bacon's dual ranking: 16th out of the 18 exact candidates, and 545th out of 177,572 on the global estimate.
*   *Decade isolation:* We generated distinct graphs for each decade rather than a cumulative graph. This allowed us to find who was genuinely central *during that specific era*. Only complete years were kept (hence the 2020-2025 period). The giant component percentage was calculated against all credited actors, including completely isolated ones.
*   *Alternatives discarded:* We considered using GraphFrames (too rigid), NetworkX (impossible on a single machine at this scale), or including TV series (would inflate the graph 10x). We also considered larger samples, but capped it to keep the job runtime reasonable (~30 minutes).

**Interpretation & The Myth:** The data confirms the cinema is a "small world": the average separation is just 4.56 steps, with 97% of pairs connected within 6 degrees. However, our graph relies on the `title.principals` table (which limits casts to ~10 headliners per film), meaning our paths are naturally slightly longer than those of the "Oracle of Bacon". The true center of Hollywood is Willem Dafoe, not Kevin Bacon. The higher separation in 2020-2025 (5.61) is likely an artifact of the shorter 6-year window and pandemic-era film production drops. Ultimately, the Kevin Bacon myth is just that—a myth. He is highly central (in the top 0.3%), but he is far from being the absolute center.
