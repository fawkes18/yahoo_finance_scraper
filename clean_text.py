import pandas as pd
import pickle
import re
import sys
from unicodedata import category
import contractions


def remove_unicode(text):
    text = text.encode('ascii', 'ignore')
    text = text.decode()
    return text


def remove_contractions(text):
    return contractions.fix(text)


def remove_emoji(text):
    regrex_pattern = re.compile(pattern = '['
                              u"\U0001F600-\U0001F64F"  # emoticons
                              u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                              u"\U0001F680-\U0001F6FF"  # transport & map symbols
                              u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                ']', flags=re.UNICODE)
    return regrex_pattern.sub(r'', text)


def remove_url(text):
    return re.sub(r"http\S+", "", text)


def remove_brackets(text):
    text = re.sub('[()]', '', text)
    text = text.replace('[', '').replace(']', '')
    return text


def normalize_whitespace(text):
    text = '\n'.join([x.strip() for x in text.splitlines()])
    text = re.compile(r'\s').sub(' ', text)
    return text.strip()


def replace_digits(text):
    return re.sub(r'\d', '0', text)


def remove_numbers(text):
    return re.compile(
    r"(?:^|(?<=[^\w,.]))[+–-]?(([1-9]\d{0,2}(,\d{3})+(\.\d*)?)|([1-9]\d{0,2}([ .]\d{3})+(,\d*)?)|(\d*?[.,]\d+)|\d+)(?:$|(?=\b))").sub('', text)


def replace_punct(text):
    return text.translate(
        dict.fromkeys(
            (i for i in range(sys.maxunicode) if category(chr(i)).startswith('P')),
            ' '
        )
    )


def remove_punct(text):
    return text.translate(dict.fromkeys(
    (i for i in range(sys.maxunicode) if category(chr(i)).startswith("P")),
    "",
    )
    )


def remove_currencies(text):
    curr = {
        "$": "USD",
        "zł": "PLN",
        "£": "GBP",
        "¥": "JPY",
        "฿": "THB",
        "₡": "CRC",
        "₦": "NGN",
        "₩": "KRW",
        "₪": "ILS",
        "₫": "VND",
        "€": "EUR",
        "₱": "PHP",
        "₲": "PYG",
        "₴": "UAH",
        "₹": "INR",
    }
    return re.compile("({})+".format("|".join(re.escape(c) for c in curr.keys()))).sub('<CUR>', text)


def lower_case(text):
    return text.lower()


def clean(text):
    if text is None:
        return ''
    else:
        text = str(text)
        text = remove_unicode(text)
        text = remove_contractions(text)
        text = remove_emoji(text)
        text = remove_url(text)
        text = remove_brackets(text)
        text = replace_digits(text)
        text = remove_numbers(text)
        text = replace_punct(text)
        text = normalize_whitespace(text)
        text = lower_case(text)
        text = remove_currencies(text)
        return text
