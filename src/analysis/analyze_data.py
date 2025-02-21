import sqlite3
from collections import Counter

import de_core_news_sm
import numpy as np
import pandas as pd
import plotly.express as px
from plotly.graph_objects import Figure
from plotly.subplots import make_subplots
from tqdm.autonotebook import tqdm
from wordcloud import WordCloud


def connect_db(func):
    def inner(self):
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.cursor()
            return func(self, cursor)

    return inner


class DataAnalyzer:
    def __init__(self, database_path: str):
        self.database_path = database_path
        self.nlp = de_core_news_sm.load()
        self.color_map = {
            "AFD": "#0088ff",
            "CDU/CSU": "#000000",
            "CDU": "#000000",
            "BÜNDNIS 90/DIE GRÜNEN": "#059142",
            "SPD": "#d30000",
            "DIE LINKE": "#cc00cc",
            "FDP": "#ffef00",
            "BSW": "#641975",
            "FRAKTIONSLOS": "#060270",
        }

    @connect_db
    def get_reden(self, cursor: sqlite3.Cursor) -> list:
        reden = []
        reden_cursor = cursor.execute(
            """
            SELECT ALL text FROM reden;
            """
        )
        for row in reden_cursor:
            # index 0 because each row contains a tuple with one element
            reden.append(row[0])

        return reden

    def tokenize_sentences(self, reden: list) -> list:
        sentences = []
        for doc in tqdm(
            self.nlp.pipe(
                reden,
                disable=[
                    "lemmatizer",
                    "ner",
                    "tagger",
                ],
                n_process=2,
            ),
            desc="Tokenizing sentences",
            total=len(reden),
        ):
            for sent in doc.sents:
                sentences.append(sent.text)

        return sentences

    def tokenize_words(self, reden: list) -> list:
        tokens = []
        for doc in tqdm(
            self.nlp.pipe(
                reden,
                disable=[
                    "ner",
                    "tagger",
                    "lemmatizer",
                    "tok2vec",
                ],
                n_process=2,
            ),
            desc="Tokenizing",
            total=len(reden),
        ):
            for token in doc:
                if (
                    token.is_alpha
                    and not token.is_stop
                    and not token.is_punct
                    and not token.is_space
                ):
                    tokens.append(token)

        return tokens

    def tokenize_nouns(self, sentences: list) -> list:
        tokens = []
        for doc in tqdm(
            self.nlp.pipe(
                sentences,
                disable=[
                    "lemmatizer",
                ],
                n_process=2,
                batch_size=500,
            ),
            desc="Tokenizing nouns",
            total=len(sentences),
        ):
            for token in doc:
                if (
                    token.is_alpha
                    and token.ent_iob_ == "O"
                    and token.pos_ == "NOUN"
                    and not token.is_stop
                ):
                    tokens.append(token)

        return tokens

    # TODO: Anreden rausfiltern
    def get_wordcloud(
        self, tokens: list, width: int, height: int, num_words: int = 200
    ) -> tuple[WordCloud, Figure]:
        word_counter = Counter(token.text.lower() for token in tokens)
        wordcloud = WordCloud(
            width=width - int(width * 0.1),
            height=height - int(height * 0.1),
            background_color="white",
            min_font_size=12,
            max_words=num_words,
        ).generate_from_frequencies(word_counter)
        word_count_range = word_counter.most_common(num_words)
        highest_occurrence = word_count_range[0][1]
        lowest_occurrence = word_count_range[-1][1]

        wordcloud_image = wordcloud.to_image()
        wordcloud_array = np.array(wordcloud_image)
        fig = px.imshow(wordcloud_array, width=width, height=height)
        fig.update_xaxes(showticklabels=False).update_yaxes(showticklabels=False)

        print(f"Range: {lowest_occurrence}-{highest_occurrence}")
        return wordcloud, fig

    def get_word_frequency(self, tokens: list) -> dict:
        word_freq = Counter(token.text.lower() for token in tokens)

        return dict(word_freq.most_common())

    @connect_db
    def get_words_per_rede(self, cursor: sqlite3.Cursor) -> pd.DataFrame:
        reden = []
        count_dict = {}
        reden_cursor = cursor.execute(
            """
            SELECT ALL rede_id, text FROM reden;
            """
        )
        for row in reden_cursor:
            reden.append((row[0], row[1]))
        for i, doc in enumerate(
            tqdm(
                self.nlp.pipe(
                    [text for _, text in reden],
                    disable=["ner", "tagger", "lemmatizer", "tok2vec", "parser"],
                    n_process=2,
                ),
                desc="Counting words",
                total=len(reden),
            )
        ):
            tokens = [
                token for token in doc if not token.is_space and not token.is_punct
            ]
            count_dict[reden[i][0]] = len(tokens)

        df = pd.DataFrame(list(count_dict.items()), columns=["rede_id", "word_count"])

        return df

    def plot_words_per_rede(self, df: pd.DataFrame) -> None:
        fig = px.box(df, x="word_count", title="Wordcount Verteilung")
        fig.show()

    @connect_db
    def count_reden_per_date(self, cursor: sqlite3.Cursor) -> pd.DataFrame:
        reden = []
        reden_cursor = cursor.execute(
            """
            SELECT ALL COUNT(r.rede_id) rede_count, s.datum FROM reden as r 
            JOIN tagesordnungspunkte AS t
            ON r.tagesordnungspunkt_id = t.tagesordnungspunkt_id
            JOIN sitzungen AS s
            ON t.sitzungs_id = s.sitzungs_id
            GROUP BY s.datum
            ORDER BY datum ASC
            """
        )
        for row in reden_cursor:
            reden.append(row)

        df = pd.DataFrame(reden, columns=["rede_count", "datum"])
        df["date"] = pd.to_datetime(df["datum"]).dt.strftime("%m-%d")
        df["year"] = pd.to_datetime(df["datum"]).dt.year
        df = df.sort_values(by="date")
        df["date"] = pd.to_datetime(df["date"], format="%m-%d")

        return df

    def plot_reden_per_date(self, df: pd.DataFrame) -> None:
        fig = px.line(
            df,
            x="date",
            y="rede_count",
            color="year",
            title="Reden pro Datum",
            markers=True,
            category_orders={"year": {2021, 2022, 2023, 2024, 2025}},
        )
        fig.update_xaxes(tickformat="%d.%m", nticks=12)

        fig.show()

    @connect_db
    def count_reden_per_fraktion(self, cursor: sqlite3.Cursor) -> pd.DataFrame:
        reden = []
        reden_cursor = cursor.execute(
            """
            SELECT ALL s.fraktion, COUNT(r.rede_id) as reden_count from reden as r
            JOIN redner as s
            ON r.redner_id = s.redner_id
            GROUP BY s.fraktion
            ORDER BY reden_count DESC
            """
        )
        for row in reden_cursor:
            reden.append(row)

        df = pd.DataFrame(reden, columns=["fraktion", "reden_count"])

        return df

    def plot_reden_per_fraktion(self, df: pd.DataFrame) -> None:
        fig = px.bar(
            df,
            x="fraktion",
            y="reden_count",
            title="Reden pro Fraktion",
            color="fraktion",
            color_discrete_map=self.color_map,
        )

        fig.show()

    @connect_db
    def get_mean_rede_length(self, cursor: sqlite3.Cursor) -> pd.DataFrame:
        reden = []
        reden_cursor = cursor.execute(
            """
            SELECT ALL s.redner_id, CONCAT(s.vorname, ' ', s.nachname) as name, r.text FROM reden as r
            JOIN redner as s
            ON r.redner_id = s.redner_id
            ORDER BY s.redner_id
            """
        )
        for row in reden_cursor:
            reden.append(row)

        df = pd.DataFrame(reden, columns=["redner_id", "name", "text"])
        df["word_count"] = [
            len([token for token in doc if not token.is_space and not token.is_punct])
            for doc in tqdm(
                self.nlp.pipe(
                    df["text"],
                    disable=["ner", "tagger", "lemmatizer", "tok2vec", "parser"],
                    n_process=2,
                ),
                desc="Counting words",
                total=len(df["text"]),
            )
        ]
        mean_word_count = (
            df.groupby(["redner_id", "name"])["word_count"].mean().reset_index()
        )
        mean_word_count.columns = ["redner_id", "name", "mean_word_count"]
        highest_mean = mean_word_count.nlargest(5, "mean_word_count")[
            ["name", "mean_word_count"]
        ]
        smallest_mean = mean_word_count.nsmallest(5, "mean_word_count")[
            ["name", "mean_word_count"]
        ]
        return mean_word_count, highest_mean, smallest_mean

    def plot_mean_rede_length(self, df: pd.DataFrame) -> None:
        fig_1 = px.histogram(df, x="mean_word_count")
        fig_2 = px.box(df, x="mean_word_count")

        fig = make_subplots(rows=2, cols=1, subplot_titles=("Histogramm", "Boxplot"))
        for trace in fig_1.data:
            fig.add_trace(trace, row=1, col=1)
        for trace in fig_2.data:
            fig.add_trace(trace, row=2, col=1)
        fig.update_layout(title_text="Mittlere Redelänge pro Redner", height=600)

        fig.show()

    @connect_db
    def get_most_frequent_commenters(self, cursor: sqlite3.Cursor) -> pd.DataFrame:
        commenters = {"name": [], "comment_count": [], "fraktion": []}
        comment_cursor = cursor.execute(
            """
            SELECT ALL kommentator, COUNT(kommentator) as kommentator_count, fraktion from kommentare
            GROUP BY kommentator
            ORDER BY kommentator_count DESC
            """
        )
        for row in comment_cursor:
            commenters["name"].append(row[0])
            commenters["comment_count"].append(row[1])
            commenters["fraktion"].append(row[2])

        df = pd.DataFrame(commenters)

        return df

    def plot_most_frequent_commenters(self, df: pd.DataFrame, num: int = 20) -> None:
        fig = px.bar(
            df[:num],
            x="name",
            y="comment_count",
            color="fraktion",
            title="Kommentare pro Kommentierer",
            color_discrete_map=self.color_map,
        )
        fig.update_layout(xaxis_categoryorder="total descending")

        fig.show()

    @connect_db
    def count_comments_per_party(self, cursor: sqlite3.Cursor) -> pd.DataFrame:
        comments = []
        comment_cursor = cursor.execute(
            """
            SELECT ALL fraktion, COUNT(fraktion) as kommentar_count from kommentare 
            GROUP BY fraktion
            ORDER BY kommentar_count
            """
        )
        for row in comment_cursor:
            comments.append(row)

        df = pd.DataFrame(comments, columns=["fraktion", "kommentar_count"])

        return df

    def plot_comments_per_party(self, df: pd.DataFrame) -> None:
        fig = px.bar(
            df,
            x="fraktion",
            y="kommentar_count",
            color="fraktion",
            color_discrete_map=self.color_map,
            title="Kommentare pro Fraktion",
        )
        fig.update_layout(xaxis_categoryorder="total descending")

        fig.show()

    @connect_db
    def count_comments_per_speaker(self, cursor: sqlite3.Cursor) -> pd.DataFrame:
        redner = []
        redner_cursor = cursor.execute(
            """
            SELECT ALL CONCAT(s.vorname, ' ', s.nachname) as redner, s.fraktion, COUNT(c.rede_id) as comment_count from kommentare as c
            JOIN reden as r
            ON c.rede_id = r.rede_id
            JOIN redner as s
            ON r.redner_id = s.redner_id
            GROUP BY s.redner_id
            ORDER BY comment_count DESC
            """
        )
        for row in redner_cursor:
            redner.append(row)

        df = pd.DataFrame(redner, columns=["redner", "fraktion", "kommentar_count"])

        return df

    def plot_comments_per_speaker(self, df: pd.DataFrame, num: int) -> None:
        df["fraktion"] = df["fraktion"].fillna("FRAKTIONSLOS")
        fig = px.bar(
            df[:num],
            x="redner",
            y="kommentar_count",
            title="Kommentare pro Redner",
            color="fraktion",
            color_discrete_map=self.color_map,
        )
        fig.update_layout(xaxis_categoryorder="total descending")

        fig.show()
