#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights & Biases
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info(f"Downloading input artifact: {args.input_artifact}")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    df = pd.read_csv(artifact_local_path)

    # Drop price outliers
    logger.info(f"Retain where price between {args.min_price} and {args.max_price} dollars")
    df = df[df["price"].between(args.min_price, args.max_price)]

    # Drop minimum_nights outliers
    logger.info(f"Retain where minimum_nights value between {args.min_nights} and {args.max_nights}")
    df = df[df["minimum_nights"].between(args.min_nights, args.max_nights)]

    # Convert last_review to datetime
    logger.info("Convert last_review to datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Create latitude/longitude boundaries
    logger.info("limit longitude and latitude to NYC area")
    df = df[df['longitude'].between(-74.25, -73.5) & df['latitude'].between(40.5, 41.2)]

    # Save file as CSV and load W&B artifact
    logger.info(f"Save cleaned data")
    df.to_csv("clean_sample.csv", index=False)

    logger.info(f"Uploading {args.output_artifact} to Weights & Biases")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

    artifact.wait()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This step cleans the data")


    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Name of input artifact in W&B",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name of output artifact uploaded into W&B",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="The type of output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Description of the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="Min price when filtering",
        required=False
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="Max price when filtering",
        required=False
    )

    parser.add_argument(
        "--min_nights",
        type=int,
        help="Min nights when filtering",
        required=False
    )

    parser.add_argument(
        "--max_nights",
        type=int,
        help="Max price when filtering",
        required=False
    )


    args = parser.parse_args()

    go(args)
