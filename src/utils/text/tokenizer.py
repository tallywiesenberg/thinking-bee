import spacy
from utils.nlp import WIKI_STOP_WORDS
from utils.nlp.cleaning import looks_like_username

nlp = spacy.load("en_core_web_sm", disable=["parser"])


def spacy_tokenizer(text):
    doc = nlp(text)
    custom_stop = WIKI_STOP_WORDS
    tokens = []
    entity_spans = set()

    # preserve named entities as full phrases
    for ent in doc.ents:
        if ent.label_ in {"PERSON", "GPE", "ORG", "NORP", "LOC", "EVENT"}:
            ent_text = ent.text.lower().replace(" ", "_")

            if (
                len(ent_text) > 2
                and ent_text not in custom_stop
                and not looks_like_username(ent_text)
            ):
                tokens.append(ent_text)

            for token in ent:
                entity_spans.add(token.i)

    # process non-entity tokens WITHOUT lemmatization
    for token in doc:
        if token.i in entity_spans:
            continue

        word = token.text.lower()

        if (
            token.is_stop
            or token.is_punct
            or not word.isalpha()
            or len(word) < 3
            or len(word) > 20
            or any(ch.isdigit() for ch in word)
            or word in custom_stop
            or looks_like_username(word)
        ):
            continue

        tokens.append(word)

    return tokens