import os
import openai

with open('/Users/marc/coding/projects/api/chatgpt/key.txt', 'r') as file:
    data = file.read().replace('\n', '')

openai.api_key = data


prompt1 = "Who are the top 20 female athletes of all time?"
completion = openai.ChatCompletion.create(
  model="gpt-3.5-turbo",
  messages=[
    {"role": "user", "content": input("You: ")}
  ]
)

print(completion.choices[0].message.content)
