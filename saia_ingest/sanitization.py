import re

punctuation = '!"#$%&\'()*+<=>[\\]^_`{|}~'


def preprocess_text(text: str, remove_punctuation=True) -> str:
    if not text:
        return ""

    # remove punctuation
    if remove_punctuation:
        text = re.sub('[%s]' % re.escape(punctuation), '  ', text)

    # non-breaking space
    text = text.replace('\xa0', ' ')

    text = text.replace("\r", " ")

    # remove tab
    text = text.replace("\t", " ")

    # Remove leading and trailing spaces on each line
    text = '\n'.join(line.strip() for line in text.split('\n'))

    # remove newlines
    text = re.sub(r'\n+', '\n', text)

    # clean up the spacing
    text = re.sub('\s{2,}', " ", text)

    # Remove extra-dots from text (in general from index references)
    text = re.sub(r'\.{4,}', '...', text)

    return text.strip()
