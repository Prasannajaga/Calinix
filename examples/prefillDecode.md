# Prefill and Decode in LLM Inference

## Core idea

In LLM inference, especially for decoder-only models like GPT, Llama, Mistral, and Qwen, inference usually has two phases:

1. Prefill
2. Decode

They both run the same Transformer model and use the same weights.

They both perform operations like:

- QKV projection
- attention
- MLP/feed-forward
- layer normalization
- output projection

But they are different in how they use the sequence.

Simple mental model:

Prefill = process the full user prompt once
Decode  = generate the answer one token at a time

For decoder-only models, prefill is not a real encoder and decode is not a separate decoder module. They are two execution phases of the same decoder-only Transformer.

---

## Why people sometimes call them encoder and decoder

People sometimes say:

prefill pod = encoder
decode pod  = decoder

This is only a loose analogy.

In classic encoder-decoder models:

- Encoder reads the input sequence
- Decoder generates the output sequence

So people call prefill “encoder-like” because it processes the input prompt.

People call decode “decoder-like” because it generates output tokens.

But in decoder-only LLMs, this is not technically exact.

Correct wording:

Prefill pod = prompt/context processing pod
Decode pod  = autoregressive token generation pod

Better sentence:

Prefill is encoder-like, but not a separate encoder.
Decode is the token-by-token generation phase.

---

## Example: user sends 100 prompt tokens

Assume the user sends one request.

Prompt:

P1, P2, P3, ..., P100

This means the prompt has 100 tokens after tokenization.

Important:

The user sends the query once.

The model then generates the answer internally token by token:

A1, A2, A3, A4, ...

Where:

P = prompt token
A = answer/generated token

---

## What happens in prefill

Prefill processes all 100 prompt tokens at once.

Input:

X_prompt = [1, 100, 4096]

Where:

1    = batch size
100  = prompt tokens
4096 = hidden size

The model computes Q, K, and V for all 100 prompt tokens:

Q_prompt = X_prompt @ Wq
K_prompt = X_prompt @ Wk
V_prompt = X_prompt @ Wv

Shapes:

Q_prompt = [1, 100, 4096]
K_prompt = [1, 100, 4096]
V_prompt = [1, 100, 4096]

Then attention scores are computed:

scores = Q_prompt @ K_prompt.T

Shape:

[1, 100, 4096] @ [1, 4096, 100] = [1, 100, 100]

This means:

100 prompt tokens attend over 100 prompt tokens

Prefill produces:

1. KV cache for prompt tokens P1...P100
2. logits/probabilities for the first output token A1

Important:

Prefill does not generate the full answer.

Prefill only predicts the first answer token.

Example:

After prefill, the model may predict:

A1 = "An"

Now the KV cache contains:

P1...P100

KV cache length = 100

---

## What is the KV cache?

KV cache stores the Key and Value tensors for previous tokens.

The model saves K and V because future tokens need to attend to previous tokens.

The model does not usually save Q.

Why?

Because each new generated token creates a new Q.

During decode:

Q_new = X_new @ Wq

But the old K and V are reused:

attention = Q_new @ K_cache.T
output    = attention @ V_cache

So prefill passes mainly:

- K cache
- V cache
- current position
- request metadata
- maybe logits for first token

In a real model, KV cache exists for every Transformer layer.

Example:

If the model has 32 layers, the cache looks roughly like:

layer_0: K, V
layer_1: K, V
layer_2: K, V
...
layer_31: K, V

---

## What happens in decode

Decode starts after prefill predicts the first output token.

Suppose prefill predicted:

A1 = "An"

Now decode uses A1 to predict A2.

Input to decode:

X_new = [1, 1, 4096]

Where:

1    = batch size
1    = one generated token
4096 = hidden size

Decode computes Q, K, and V only for this one token:

Q_new = X_new @ Wq
K_new = X_new @ Wk
V_new = X_new @ Wv

Shapes:

Q_new = [1, 1, 4096]
K_new = [1, 1, 4096]
V_new = [1, 1, 4096]

Then the new K and V are appended to the existing cache.

Before decode step 1:

KV cache = P1...P100
cache length = 100

After appending A1:

KV cache = P1...P100, A1
cache length = 101

Now attention is:

scores_new = Q_new @ K_cache.T

Shape:

[1, 1, 4096] @ [1, 4096, 101] = [1, 1, 101]

This means:

1 current generated token attends over 101 cached tokens

Those 101 tokens are:

100 prompt tokens + 1 generated token

The model then predicts:

A2

Then decode repeats.

---

## Decode timeline

User sends:

P1...P100

Prefill:

process P1...P100
build KV cache length 100
produce logits for A1
sample/select A1

Decode step 1:

input A1
use KV cache P1...P100
produce logits for A2
sample/select A2
cache becomes P1...P100,A1

Decode step 2:

input A2
use KV cache P1...P100,A1
produce logits for A3
sample/select A3
cache becomes P1...P100,A1,A2

Decode step 3:

input A3
use KV cache P1...P100,A1,A2
produce logits for A4
sample/select A4
cache becomes P1...P100,A1,A2,A3

This continues until:

- end-of-sequence token
- max tokens reached
- stop sequence matched
- user cancels

---

## Why decode generates one token at a time

LLMs are autoregressive.

That means:

the next token depends on all previous tokens

Formula:

P(answer) =
P(A1 | prompt)
x P(A2 | prompt, A1)
x P(A3 | prompt, A1, A2)
x P(A4 | prompt, A1, A2, A3)
...

So the model cannot generate A2 before A1 is chosen.

Example:

Prompt:

"Explain L7 load balancing in simple words."

Possible first tokens:

"An"
"The"
"A"

The second token depends on which first token was selected.

If A1 = "An", then A2 may be "L7"
If A1 = "The", then A2 may be "load"
If A1 = "A", then A2 may be "Layer"

So future tokens depend on previous generated tokens.

That is why decode is sequential.

---

## Why prefill can process the prompt in parallel

The full prompt is already known.

For example:

P1, P2, P3, ..., P100

Since all prompt tokens are available, the model can compute Q, K, and V for all 100 prompt tokens in one big parallel matrix operation.

Prefill:

X_prompt = [1, 100, 4096]

Q = X_prompt @ Wq
K = X_prompt @ Wk
V = X_prompt @ Wv

This computes Q/K/V for all 100 tokens at once.

That is why prefill is sequence-parallel.

---

## Why decode cannot process all answer tokens in parallel

The answer tokens are not known yet.

To generate:

A1, A2, A3, A4

The model must do:

predict A1
then use A1 to predict A2
then use A2 to predict A3
then use A3 to predict A4

It cannot compute A1, A2, A3, A4 all at the same time because A2 depends on A1, A3 depends on A2, and so on.

---

## Matrix shape comparison

Assume:

batch size = 1
prompt length = 100
hidden size = 4096

### Prefill

Input:

X_prompt = [1, 100, 4096]

Q/K/V:

Q = [1, 100, 4096]
K = [1, 100, 4096]
V = [1, 100, 4096]

Attention:

Q @ K.T

[1, 100, 4096] @ [1, 4096, 100] = [1, 100, 100]

Meaning:

100 prompt tokens attend over 100 prompt tokens

### Decode step 1

Input:

X_new = [1, 1, 4096]

Q/K/V for new token:

Q_new = [1, 1, 4096]
K_new = [1, 1, 4096]
V_new = [1, 1, 4096]

After appending K/V to cache:

K_cache = [1, 101, 4096]
V_cache = [1, 101, 4096]

Attention:

Q_new @ K_cache.T

[1, 1, 4096] @ [1, 4096, 101] = [1, 1, 101]

Meaning:

1 generated token attends over 101 tokens

### Decode step 2

Cache length becomes 102.

Attention shape:

[1, 1, 102]

### Decode step 3

Cache length becomes 103.

Attention shape:

[1, 1, 103]

So:

Prefill attention shape = [100, 100]
Decode attention shape  = [1, 101], then [1, 102], then [1, 103], ...

---

## Same operations, different workload

Both prefill and decode do:

Q = X @ Wq
K = X @ Wk
V = X @ Wv
attention = softmax(Q @ K.T) @ V
MLP
output projection

But the input shape is different.

Prefill:

X = [batch, prompt_tokens, hidden]

Decode:

X = [batch, 1, hidden]

So the operation is similar, but the execution pattern is different.

Prefill = large parallel matrix work
Decode  = small repeated matrix work

---

## Why systems separate prefill and decode pods

Production inference systems sometimes separate prefill and decode into different pods/workers because they have different bottlenecks.

### Prefill characteristics

Prefill is:

- compute-heavy
- large matrix multiplication
- good GPU utilization
- processes input tokens
- affects time to first token

Prefill is measured by:

input tokens/sec
time to first token
prompt processing latency

### Decode characteristics

Decode is:

- memory-bandwidth-heavy
- KV-cache-heavy
- latency-sensitive
- sequential
- generates output tokens
- affects streaming speed

Decode is measured by:

output tokens/sec
time per output token
active sequences
KV cache memory usage

---

## TTFT and TPOT

Two important latency metrics:

TTFT = Time To First Token
TPOT = Time Per Output Token

Prefill mostly affects TTFT.

Long prompt = more prefill work = higher TTFT

Decode mostly affects TPOT.

Slow decode = slow streaming output

Example:

User sends a long prompt.

Prefill must process all prompt tokens before the first answer token appears.

Once first token appears, decode controls how fast the rest of the answer streams.

---

## Why separating helps production systems

If prefill and decode run on the same GPU without control, a huge prompt can block ongoing decode streams.

Example:

100 users are already receiving streamed tokens.

Then one user sends a 50,000-token prompt.

If the same GPU handles this huge prefill immediately, decode may stall.

Users may see:

token...
pause...
pause...
next token

That is bad user experience.

With separation:

huge prompt goes to prefill pod
active streams stay on decode pod

So token streaming remains smooth.

---

## Scaling separately

Different workloads need different capacity.

Example 1:

RAG system with huge context and short answer

Needs more prefill capacity because input tokens are high.

Example 2:

Chatbot with short prompts and long answers

Needs more decode capacity because output tokens are high.

Separate prefill/decode allows separate autoscaling:

scale prefill pods by input tokens/sec
scale decode pods by output tokens/sec and active sequences

---

## What is passed from prefill pod to decode pod

Conceptually, prefill passes:

- request ID
- KV cache for prompt tokens
- current sequence length / position
- last token or first sampled output token
- sampling metadata
- model/session metadata

Example:

prefill_output = {
    "request_id": "abc123",
    "kv_cache": {
        "layer_0": {
            "K": K_layer_0,
            "V": V_layer_0
        },
        "layer_1": {
            "K": K_layer_1,
            "V": V_layer_1
        }
    },
    "position": 100,
    "first_token": A1
}

Decode receives this and continues generation.

In real systems, the KV cache may not always be copied directly over the network. It may be transferred GPU-to-GPU, stored in cache blocks, or managed by a disaggregated inference runtime.

---

## Minimal pseudocode

### Prefill

input prompt tokens:

P1...P100

run model on full prompt:

X_prompt = embeddings(P1...P100)

Q = X_prompt @ Wq
K = X_prompt @ Wk
V = X_prompt @ Wv

scores = Q @ K.T
out = softmax(scores) @ V

save:

KV cache = K, V

produce:

logits for A1

sample:

A1

### Decode

current token:

A1

old cache:

K/V for P1...P100

run model on one token:

X_new = embedding(A1)

Q_new = X_new @ Wq
K_new = X_new @ Wk
V_new = X_new @ Wv

append:

K_cache = concat(K_cache, K_new)
V_cache = concat(V_cache, V_new)

attention:

scores = Q_new @ K_cache.T
out = softmax(scores) @ V_cache

produce:

logits for A2

sample:

A2

repeat with A2, then A3, then A4...

---

## Minimal PyTorch-style example

import torch
import torch.nn.functional as F
import math

batch = 1
prompt_len = 100
hidden = 4096
head_dim = 4096

Wq = torch.randn(hidden, head_dim)
Wk = torch.randn(hidden, head_dim)
Wv = torch.randn(hidden, head_dim)
Wo = torch.randn(head_dim, hidden)

# -------------------------
# Prefill
# -------------------------

X_prompt = torch.randn(batch, prompt_len, hidden)

Q = X_prompt @ Wq
K = X_prompt @ Wk
V = X_prompt @ Wv

scores = Q @ K.transpose(-2, -1) / math.sqrt(head_dim)

# Causal mask so tokens cannot attend to future prompt positions
causal_mask = torch.tril(torch.ones(prompt_len, prompt_len))
scores = scores.masked_fill(causal_mask == 0, float("-inf"))

attn = F.softmax(scores, dim=-1)

out = attn @ V
out = out @ Wo

kv_cache = {
    "K": K,
    "V": V
}

print("PREFILL")
print("X_prompt:", X_prompt.shape)  # [1, 100, 4096]
print("Q:", Q.shape)                # [1, 100, 4096]
print("K:", K.shape)                # [1, 100, 4096]
print("V:", V.shape)                # [1, 100, 4096]
print("scores:", scores.shape)      # [1, 100, 100]

# Pretend prefill sampled the first generated token A1
X_new = torch.randn(batch, 1, hidden)

# -------------------------
# Decode
# -------------------------

Q_new = X_new @ Wq
K_new = X_new @ Wk
V_new = X_new @ Wv

K_all = torch.cat([kv_cache["K"], K_new], dim=1)
V_all = torch.cat([kv_cache["V"], V_new], dim=1)

scores_new = Q_new @ K_all.transpose(-2, -1) / math.sqrt(head_dim)

attn_new = F.softmax(scores_new, dim=-1)

out_new = attn_new @ V_all
out_new = out_new @ Wo

kv_cache["K"] = K_all
kv_cache["V"] = V_all

print("DECODE")
print("X_new:", X_new.shape)          # [1, 1, 4096]
print("Q_new:", Q_new.shape)          # [1, 1, 4096]
print("K_new:", K_new.shape)          # [1, 1, 4096]
print("V_new:", V_new.shape)          # [1, 1, 4096]
print("K_all:", K_all.shape)          # [1, 101, 4096]
print("V_all:", V_all.shape)          # [1, 101, 4096]
print("scores_new:", scores_new.shape)# [1, 1, 101]

---

## Final summary

Prefill and decode use the same Transformer model and the same matrix operations.

The difference is not the operation.

The difference is the sequence shape and execution pattern.

Prefill:

- processes the whole prompt at once
- computes Q/K/V for all prompt tokens
- builds KV cache
- predicts the first output token
- parallel over prompt sequence
- compute-heavy
- affects time to first token

Decode:

- processes one generated token at a time
- computes Q/K/V only for the new token
- reuses old K/V from KV cache
- appends new K/V to cache
- predicts the next token
- sequential over output tokens
- memory/KV-cache-heavy
- affects streaming speed

Best mental model:

Prefill = read/process the prompt and build context.
Decode  = write the answer one token at a time using that context.

For decoder-only LLMs:

Prefill is not a real encoder.
Decode is not a separate decoder module.
Both are the same decoder-only Transformer running in different phases.