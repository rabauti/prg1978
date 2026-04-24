import shutil
import subprocess
import uuid
from pathlib import Path

GRAPHVIZ_BINARY = "dot"
DEFAULT_OUTPUT_FORMAT = "svg"


def resolve_graph_output(
    filename=None,
    output_dir=None,
    output_format=DEFAULT_OUTPUT_FORMAT,
):
    normalized_format = (output_format or DEFAULT_OUTPUT_FORMAT).lstrip(".")

    if filename:
        output_path = Path(filename)
        if output_dir and not output_path.is_absolute():
            output_path = Path(output_dir) / output_path
        if not output_path.suffix:
            output_path = output_path.with_suffix(f".{normalized_format}")
        else:
            normalized_format = output_path.suffix.lstrip(".")
    else:
        if output_dir is None:
            raise ValueError("output_dir is required when filename is not provided.")
        target_dir = Path(output_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        output_path = (
            target_dir / f"syntax_graph_{uuid.uuid4().hex}.{normalized_format}"
        )

    return output_path, normalized_format


def render_dot(
    dot_source,
    filename=None,
    output_dir=None,
    output_format=DEFAULT_OUTPUT_FORMAT,
    announce=True,
):
    dot_binary = shutil.which(GRAPHVIZ_BINARY)
    if not dot_binary:
        raise RuntimeError(
            "Graphviz 'dot' executable was not found. Install Graphviz or render the DOT manually."
        )

    output_path, normalized_format = resolve_graph_output(
        filename=filename,
        output_dir=output_dir,
        output_format=output_format,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if normalized_format == "dot":
        output_path.write_text(dot_source, encoding="utf-8")
    else:
        subprocess.run(
            [dot_binary, f"-T{normalized_format}", "-o", str(output_path)],
            input=dot_source,
            text=True,
            check=True,
        )

    if announce:
        print(f"Graph written to {output_path}")
    return output_path


def render_syntax_graph(
    graph,
    filename=None,
    output_dir=None,
    output_format=DEFAULT_OUTPUT_FORMAT,
    title=None,
    highlight=None,
    custom_colors=None,
    announce=True,
):
    dot_source = graph.to_dot(
        title=title,
        highlight=highlight,
        custom_colors=custom_colors,
    )
    return render_dot(
        dot_source,
        filename=filename,
        output_dir=output_dir,
        output_format=output_format,
        announce=announce,
    )
