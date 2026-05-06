import pandas as pd


def enrich_collection_with_metadata(
    df_collected,
    metadata_file,
    collected_uid_column,
    metadata_uid_column,
    output_file=None,
    metadata_delimiter="\t",
    output_delimiter="\t",
):
    if collected_uid_column not in df_collected.columns:
        raise ValueError(f"Missing collected uid column: {collected_uid_column}")

    metadata_df = pd.read_csv(
        metadata_file, sep=metadata_delimiter, dtype=str
    ).fillna("")
    metadata_subset = metadata_df.drop_duplicates(subset=[metadata_uid_column]).copy()
    df_enriched = df_collected.copy()
    df_enriched[collected_uid_column] = df_enriched[collected_uid_column].astype(str)
    metadata_subset[metadata_uid_column] = metadata_subset[metadata_uid_column].astype(
        str
    )
    metadata_columns = list(metadata_subset.columns)

    if metadata_uid_column == collected_uid_column:
        df_enriched = df_enriched.merge(
            metadata_subset,
            how="left",
            on=metadata_uid_column,
            suffixes=("_collected", ""),
        )
    else:
        df_enriched = df_enriched.merge(
            metadata_subset,
            how="left",
            left_on=collected_uid_column,
            right_on=metadata_uid_column,
            suffixes=("_collected", ""),
        )

    ordered_columns = metadata_columns + []
    for column in df_collected.columns:
        if metadata_uid_column == collected_uid_column and column == collected_uid_column:
            continue
        if column in metadata_columns:
            ordered_columns.append(f"{column}_collected")
        else:
            ordered_columns.append(column)

    df_enriched = df_enriched[ordered_columns]

    if output_file is not None:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df_enriched.to_csv(output_file, index=None, sep=output_delimiter)

    return df_enriched


__all__ = ["enrich_collection_with_metadata"]
