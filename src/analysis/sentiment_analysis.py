import os
import re

import torch
from germansentiment import SentimentModel
from transformers import AutoModelForSequenceClassification, AutoTokenizer


class HuggingFaceSentimentAnalyzer:

    def __init__(self):
        self.model = AutoModelForSequenceClassification.from_pretrained(
            "ssary/XLM-RoBERTa-German-sentiment"
        )
        self.tokenizer = AutoTokenizer.from_pretrained(
            "ssary/XLM-RoBERTa-German-sentiment"
        )

    def analyze_sentence(self, sentence: str):
        inputs = self.tokenizer(
            sentence, return_tensors="pt", truncation=True, max_length=512
        )
        with torch.no_grad():
            outputs = self.model(**inputs)
        predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
        sentiment_classes = ["negative", "neutral", "positive"]

        return sentiment_classes[predictions.argmax()]

    def analyze_sentence_list(self, sentences: list) -> list:
        sentiments = []
        for sentence in sentences:
            sentiment = self.analyze_sentence(sentence)
            sentiments.append(sentiment)

        return sentiments


class GermanSentimentAnalyzer:

    def __init__(self):
        self.model = SentimentModel()

    def analyze_sentences(self, sentences: list | str) -> list:
        if isinstance(sentences, str):
            return self.model.predict_sentiment([sentences])

        return self.model.predict_sentiment(sentences)


class LookupSentimentAnalyzer:

    def __init__(self, dir_path: str):
        self.positive = self.parse_sentiment_file(
            os.path.join(dir_path, "SentiWS_v1.8c_Positive.txt")
        )
        self.negative = self.parse_sentiment_file(
            os.path.join(dir_path, "SentiWS_v1.8c_Negative.txt")
        )

    def parse_sentiment_file(self, file_path: str) -> dict:
        sentiment_dict = {}
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

                    sentiment_dict[word] = score
                    for alt in alternatives:
                        if alt:
                            sentiment_dict[alt] = score

        return sentiment_dict

    def analyze_sentence(self, sentence: str) -> float:
        score = 0.0
        words = re.sub(r"[^\w\s]", "", sentence).split()
        num_words = len(words)
        found_words = 0
        for word in words:
            if word in self.negative:
                found_words += 1
                score += float(self.negative[word])
            elif word in self.positive:
                found_words += 1
                score += float(self.positive[word])

        score = score * (found_words / num_words)

        return round(score, 4)

    def analyze_sentence_list(self, sentences: list) -> list:
        sentiments = []
        for sentence in sentences:
            sentiment = self.analyze_sentence(sentence)
            sentiments.append(sentiment)

        return sentiments
