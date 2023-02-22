
import base64
from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd
import requests
from wordcloud import STOPWORDS, WordCloud

from dagster import asset, get_dagster_logger, MetadataValue, Output

@asset
def topstory_ids():
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_new_story_ids = requests.get(newstories_url).json()[:100]
    return top_new_story_ids

@asset(
    io_manager_key="database_io_manager"
)
def topstories(topstory_ids):
    logger = get_dagster_logger()

    results = []
    for item_id in topstory_ids:
        item = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
        ).json()
        results.append(item)

        if len(results) % 20 == 0:
            logger.info(f"Got {len(results)} items so far.")

    df = pd.DataFrame(results).drop(["kids"], axis=1)

    return Output(
        value=df,
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )


@asset
def topstories_word_cloud(topstories):
    stopwords = set(STOPWORDS)
    stopwords.update(["Ask", "Show", "HN", "S"])
    titles_text = " ".join([str(item) for item in topstories["title"]])
    titles_cloud = WordCloud(stopwords=stopwords, background_color="white").generate(
        titles_text
    )

    # Generate the word cloud image
    plt.figure(figsize=(8, 8), facecolor=None)
    plt.imshow(titles_cloud, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout(pad=0)

    # Convert the image to a saveable format
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())

    # Convert the image to Markdown to preview it within Dagster
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    # Attach the Markdown content as metadata to the asset
    return Output(
        value=image_data,
        metadata={
            "plot": MetadataValue.md(md_content)
        }
    )
