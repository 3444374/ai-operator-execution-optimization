"""A tiny real Hugging Face tokenizer, no model weights or downloads."""
from tokenizers import Tokenizer
from tokenizers.models import WordLevel
from tokenizers.pre_tokenizers import Whitespace
from transformers import PreTrainedTokenizerFast

TEMPLATE = "{% for message in messages %}{{ message['role'] ~ ': ' ~ message['content'] ~ '\\n' }}{% endfor %}{% if add_generation_prompt %}assistant:{% endif %}"

def tokenizer():
    value = Tokenizer(WordLevel({'[UNK]':0}, unk_token='[UNK]'))
    value.pre_tokenizer = Whitespace()
    return PreTrainedTokenizerFast(tokenizer_object=value, unk_token='[UNK]', chat_template=TEMPLATE)
