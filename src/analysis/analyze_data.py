import nltk, json
from nltk.tokenize import word_tokenize, sent_tokenize


class DataAnalyzer:
    def __init__(self, data_path: str):
        with open(data_path, "r", encoding="utf-8") as file:
            self.data = json.load(file)

    def extract_paragraphs(self, data: dict | list, key: str):
        paragraphs = []
        if isinstance(data, dict):
            for key, value in data.items():
                if key == key and isinstance(value, str):
                    paragraphs.append(value)
                else:
                    paragraphs.extend(self.extract_paragraphs(value))
        elif isinstance(data, list):
            for item in data:
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

        # TODO: remove stopwords
        return word_frequency

    def num_comments_per_speaker(self, data: dict | list, target_redner: str):
        comments = 0
        if isinstance(data, dict):
            for key, value in data.items():
                comments += self.num_comments_per_speaker(value, target_redner)
        elif isinstance(data, list):
            for item in data:
                if item.get("redner") == target_redner:
                    for item in data:
                        if list(item.keys())[0] == "kommentar":
                            comments += 1
                else:
                    for key, value in item.items():
                        comments += self.num_comments_per_speaker(value, target_redner)

        return comments
