import nltk, json
from nltk.tokenize import word_tokenize, sent_tokenize


class DataAnalyzer:
    def __init__(self, data_path: str):
        with open(data_path, "r", encoding="utf-8") as f:
            self.data = json.load(f)

    def extract_paragraphs(self, data: json, key: str):
        paragraphs = []
        if isinstance(self.data, dict):
            for key, value in self.data.items():
                if key == key and isinstance(value, str):
                    paragraphs.append(value)
                else:
                    paragraphs.extend(self.extract_paragraphs(value))
        elif isinstance(self.data, list):
            for item in self.data:
                paragraphs.extend(self.extract_paragraphs(item))

        return paragraphs

    def tokenize_words(self, key: str):
        paragraphs = self.extract_paragraphs(self.data, key)
        tokens = []
        for paragraph in paragraphs:
            sentences = sent_tokenize(paragraph)
            for sentence in sentences:
                words = word_tokenize(sentence)
                tokens.extend(words)
        return tokens

    def word_frequency_dist(self, tokens: list):
        word_frequency = nltk.FreqDist(tokens)

        return word_frequency

    def num_comments_per_speaker_dist(self):
        pass
