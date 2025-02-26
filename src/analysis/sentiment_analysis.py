import os
import re

import de_core_news_sm
import torch
from germansentiment import SentimentModel
from transformers import AutoModelForSequenceClassification, AutoTokenizer


class HuggingFaceSentimentAnalyzer:

    def __init__(self):
        if torch.cuda.is_available():
            self.device = "cuda"
        else:
            self.device = "cpu"
        self.model = AutoModelForSequenceClassification.from_pretrained(
            "ssary/XLM-RoBERTa-German-sentiment"
        )
        self.tokenizer = AutoTokenizer.from_pretrained(
            "ssary/XLM-RoBERTa-German-sentiment"
        )

    def predict_sentiment(self, text: list[str]):
        inputs = self.tokenizer.batch_encode_plus(
            text,
            return_tensors="pt",
            truncation=True,
            add_special_tokens=True,
            max_length=512,
        ).to(self.device)
        with torch.no_grad():
            outputs = self.model(**inputs)
        predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
        print(predictions)
        sentiment_classes = ["negative", "neutral", "positive"]

        return [sentiment_classes[prediction.argmax()] for prediction in predictions]

    def analyze_sentence_list(self, sentences: list | str, batch_size: int) -> list:
        if isinstance(sentences, str):
            return self.predict_sentiment([sentences])

        sentiments = []
        for i in range(0, len(sentences), batch_size):
            batch = sentences[i : i + batch_size]
            batch_sentiments = self.predict_sentiment(batch)
            sentiments.extend(batch_sentiments)

        return sentiments


class GermanSentimentAnalyzer:

    def __init__(self):
        self.model = SentimentModel()

    def analyze_sentence_list(self, sentences: list | str, batch_size: int) -> list:
        if isinstance(sentences, str):
            return self.model.predict_sentiment([sentences])

        sentiments = []
        for i in range(0, len(sentences), batch_size):
            batch = sentences[i : i + batch_size]
            batch_sentiments = self.model.predict_sentiment(batch)
            sentiments.extend(batch_sentiments)
        return sentiments


class LookupSentimentAnalyzer:

    def __init__(self, dir_path: str):
        self.sentiment = self.parse_sentiment_files(
            os.path.join(dir_path, "SentiWS_v1.8c_Positive.txt"),
            os.path.join(dir_path, "SentiWS_v1.8c_Negative.txt"),
        )
        self.nlp = de_core_news_sm.load()

    def parse_sentiment_files(
        self, positive_file_path: str, negative_file_path: str
    ) -> dict:
        sentiment_dict = {}
        for file_path in [positive_file_path, negative_file_path]:
            with open(file_path, "rt", encoding="utf-8") as file:
                for line in file:
                    parts = line.split("|")
                    if len(parts) > 1:
                        word_info = parts[1].split(maxsplit=2)
                        if len(word_info) > 2:
                            word, score, alternatives = (
                                parts[0],
                                word_info[1],
                                re.sub(r"\s", "", word_info[2]).split(","),
                            )
                        else:
                            word, score, alternatives = parts[0], word_info[1], []

                        sentiment_dict[word] = float(score)
                        for alt in alternatives:
                            if alt:
                                sentiment_dict[alt] = float(score)
        return sentiment_dict

    def analyze_sentence(self, sentence: str) -> float:
        score = 0.0
        words = []
        doc = self.nlp(sentence)
        for token in doc:
            if token.is_alpha:
                words.append(token.text)
        num_words = len(words)
        found_words = 0
        for word in words:
            if word in self.sentiment:
                found_words += 1
                score += self.sentiment[word]
            # score += self.sentiment.get(word, 0.0)

        score = score * (found_words / num_words) / num_words

        return score

    def analyze_sentence_list(self, sentences: list, _: int) -> list:
        sentiments = []
        for sentence in sentences:
            sentiment = self.analyze_sentence(sentence)
            sentiments.append(sentiment)

        return sum(sentiments)
