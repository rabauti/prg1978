import pandas as pd


def enrich_collection_with_metadata(
    df_collected,
    metadata_file,
    collected_uid_column,
    metadata_uid_column,
    metadata_columns_to_add,
    output_file=None,
    metadata_delimiter="\t",
    output_delimiter="\t",
):
    if not metadata_columns_to_add:
        return df_collected

    if collected_uid_column not in df_collected.columns:
        raise ValueError(f"Missing collected uid column: {collected_uid_column}")

    metadata_df = pd.read_csv(
        metadata_file, sep=metadata_delimiter, dtype=str
    ).fillna("")
    metadata_subset = metadata_df[
        [metadata_uid_column, *metadata_columns_to_add]
    ].drop_duplicates(
        subset=[metadata_uid_column]
    ).copy()
    df_enriched = df_collected.copy()
    df_enriched[collected_uid_column] = df_enriched[collected_uid_column].astype(str)
    metadata_subset[metadata_uid_column] = metadata_subset[metadata_uid_column].astype(
        str
    )

    df_enriched = df_enriched.merge(
        metadata_subset,
        how="left",
        left_on=collected_uid_column,
        right_on=metadata_uid_column,
    )
    if metadata_uid_column != collected_uid_column:
        df_enriched.drop(columns=[metadata_uid_column], inplace=True)

    if output_file is not None:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df_enriched.to_csv(output_file, index=None, sep=output_delimiter)

    return df_enriched


__all__ = ["enrich_collection_with_metadata"]
