import sqlite3
import spacy
import de_core_news_sm
from collections import Counter
from tqdm.notebook import tqdm


class DataAnalyzer:
    def __init__(self, database_path: str):
        self.database_path = database_path
        self.nlp = de_core_news_sm.load()

    def connect_db(func):
        def inner(self):
            with sqlite3.connect(self.database_path) as conn:
                cursor = conn.cursor()
                return func(self, cursor)

        return inner

    @connect_db
    def get_reden(self, cursor) -> list:
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

    @connect_db
    def get_reden_with_id(self, cursor) -> dict:
        reden = {}
        reden_cursor = cursor.execute(
            """
            SELECT ALL rede_id, text FROM reden;
            """
        )
        for row in reden_cursor:
            reden[row[0]] = row[1]

    def tokenize_words(self, reden: list) -> dict:
        tokens = []
        # tqdm wrapper
        for doc in tqdm(
            self.nlp.pipe(reden, disable=["ner", "tagging"]), desc="Tokenizing"
        ):
            for token in doc:
                if not token.is_stop and not token.is_punct:
                    tokens.append(token)

        return tokens

    def get_word_frequency(self, tokens: list) -> dict:
        word_freq = Counter()
        word_freq = (
            Counter(
                token.text.lower()
                for token in tokens
                if token.is_alpha and not token.is_stop
            )
            + word_freq
        )
        return dict(word_freq.most_common())

    def get_words_per_rede(self) -> dict:
        count_dict = {}
        reden_dict = self.get_reden_with_id()
        for key, value in reden_dict.items():
            tokens = self.tokenize_words(value)
            count_dict["key"] = len(tokens)
        return count_dict
