import numpy as np
import re

from utils.text import NGRAM_RANGE

def top_words_for_component(nmf, terms, topic_idx, top_n=3):
    weights = nmf.components_[topic_idx]
    top_idx = np.argsort(np.abs(weights))[::-1][:top_n]
    return ", ".join(terms[j] for j in top_idx)

def replace_nmf_with_keywords(feature_name, text_models, top_n=3):
    pattern = r"(talk_diff|talk_comment)_nmf_(\d+)"

    def repl(match):
        channel = match.group(1)
        topic_idx = int(match.group(2))

        if channel not in text_models:
            return match.group(0)

        model_obj = text_models[channel]

        words = top_words_for_component(
            model_obj["nmf"],
            model_obj["terms"],
            topic_idx,
            top_n=top_n
        )

        return f"{channel}: ({words})"

    return re.sub(pattern, repl, feature_name)

from sklearn.decomposition import NMF
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd

def build_text_nmf_features_safe(
    rev_text,
    text_col,
    prefix,
    tokenizer,
    max_features=300,
    n_components=10,
    min_df=2,
    max_df=0.8,
    ngram_range=NGRAM_RANGE,
):
    rev_text = rev_text.copy()
    docs = rev_text[text_col].fillna("").astype(str)

    # basic empty check
    nonempty = docs.str.strip().ne("")
    rev_text = rev_text.loc[nonempty].copy()
    docs = docs.loc[nonempty]

    if len(docs) < 5:
        print(f"Skipping {prefix}: too few documents ({len(docs)})")
        return None, None, None, None, []

    vectorizer = TfidfVectorizer(
        max_features=max_features,
        tokenizer=tokenizer,
        token_pattern=None,
        lowercase=False,
        min_df=min_df,
        max_df=max_df,
        stop_words=None,
        ngram_range=ngram_range,
    )

    try:
        X_tfidf = vectorizer.fit_transform(docs)
    except ValueError as e:
        print(f"Skipping {prefix}: {e}")
        return None, None, None, None, []

    if X_tfidf.shape[1] < 2:
        print(f"Skipping {prefix}: only {X_tfidf.shape[1]} terms survived")
        return None, None, None, None, []

    terms = vectorizer.get_feature_names_out()

    n_components = min(n_components, X_tfidf.shape[1])

    nmf = NMF(
        n_components=n_components,
        init="nndsvda",
        random_state=42,
        max_iter=1000,
    )

    X_nmf = nmf.fit_transform(X_tfidf)

    cols = [f"{prefix}_nmf_{i}" for i in range(X_nmf.shape[1])]

    nmf_df = pd.DataFrame(
        X_nmf,
        columns=cols,
        index=rev_text.index
    )

    print(
        f"{prefix}: docs={len(docs)}, "
        f"terms={X_tfidf.shape[1]}, "
        f"components={len(cols)}"
    )

    return nmf_df, vectorizer, nmf, terms, cols

def get_nmf_topic_keywords(nmf, terms, topic_idx, top_n=15):
    weights = nmf.components_[topic_idx]
    top_idx = weights.argsort()[::-1][:top_n]

    return pd.DataFrame({
        "term": terms[top_idx],
        "weight": weights[top_idx]
    })

def get_nmf_topic_strengths(nmf):
    return np.linalg.norm(nmf.components_, axis=1)

def alpha_from_strength(strength, min_alpha=0.25, max_alpha=1.0):
    """Map a topic strength to a matplotlib alpha value.

    Scales strength linearly to the [min_alpha, max_alpha] range based on
    the global strength_min and strength_max values.

    Args:
        strength (float): Topic strength to convert.
        min_alpha (float): Minimum alpha for the weakest topic.
        max_alpha (float): Maximum alpha for the strongest topic.

    Returns:
        float: Alpha value to use for plotting.
    """
    if strength_max == strength_min:
        return max_alpha
    scaled = (strength - strength_min) / (strength_max - strength_min)
    return min_alpha + scaled * (max_alpha - min_alpha)