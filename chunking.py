import base64
import logging
import re
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter
import tiktoken
import trafilatura

logger = logging.getLogger(__name__)

# Chunking strategy constants
CHUNKING_STRATEGY_FIXED = "fixed_size"
CHUNKING_STRATEGY_H1_BASED = "h1_heading_based"
CHUNKING_STRATEGY_H2_BASED = "h2_heading_based"

def get_html_content(html_doc):
    """Decode base64-encoded HTML content from document dict"""
    file_data = html_doc["file_data"]["data"]
    decoded_file_data = base64.b64decode(file_data)

    try:
        html_content = decoded_file_data.decode("utf-8")
    except UnicodeDecodeError:
        logger.info(f"UnicodeDecodeError... File Location: {html_doc['metadata_storage_path']}")
        html_content = decoded_file_data.decode("windows-1252")

    return html_content

def get_metadata_paths(html_content, storage_path):
    """Extract metadata paths from <meta name='category'> in raw HTML string"""
    soup = BeautifulSoup(html_content, "html.parser")
    meta_data_element = soup.find("meta", attrs={"name": "category"})
    if meta_data_element:
        meta_data_paths = meta_data_element.get("content", "")
        return re.findall(r"['\"]([^'\"]*)['\"]", meta_data_paths)

    logger.info(f"No Metadata... File Location: {storage_path}")
    return []

def get_html_title(html_content, storage_path):
    """Extract title from HTML <title> tag in raw HTML string"""
    soup = BeautifulSoup(html_content, "html.parser")
    title_element = soup.find("title")
    if title_element:
        return title_element.get_text(strip=True)

    logger.info(f"No title found... File Location: {storage_path}")
    return "Untitled"

def count_tokens(text):
    """Count tokens using tiktoken"""
    encoding = tiktoken.encoding_for_model("gpt-4")
    return len(encoding.encode(text))

def extract_html_main_content(html_content, extract_main_content=False):
    """
    Extract main content from HTML using Trafilatura.
    
    Args:
        html_content (str): Raw HTML string.
        extract_main_content (bool): If True, use Trafilatura to extract only the main content
                                     (removes navigation, TOC, boilerplate, etc.)
    """
    if not extract_main_content:
        # Return the raw HTML unchanged
        return html_content

    extracted_text = trafilatura.extract(
        html_content,
        include_comments=False,
        include_tables=True,
        include_formatting=True,
        favor_recall=True,
        output_format='html'
    )

    return extracted_text if extracted_text else html_content

def html_to_markdown(html_str, heading_style="ATX"):
    """Convert HTML string to Markdown string using configurable heading style (default: ATX #)"""
    return md(html_str, heading_style=heading_style)

def fixed_size_chunking(markdown_content, chunk_size=750, chunk_overlap=100):
    """Fixed-size chunking using RecursiveCharacterTextSplitter"""
    split_texts = RecursiveCharacterTextSplitter().from_tiktoken_encoder(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap
    ).split_text(markdown_content)
    
    # Create chunks with the expected dictionary format
    chunks = [{"text": t.strip(), "heading": ""} for t in split_texts if t and t.strip()]
    
    return chunks

def h1_heading_based_chunking(markdown_content, chunk_size=750, chunk_overlap=100, threshold=50):
    """
    H1 heading-based chunking with smart splitting:
    - Split primarily on H1 (#) headings
    - If no H1 found, fall back to H2 (##) headings
    - If no headings at all, use fixed-size chunking
    - When content exceeds token limit, split with continuation markers
    - If remaining content is less than threshold tokens, include it in current chunk
    """
    # First, try to split on H1 headings
    header_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=[
            ("#", "H1"),
        ]
    )
    documents = header_splitter.split_text(markdown_content)
    
    # Check if we actually got H1-based splits
    has_meaningful_h1_splits = (
        len(documents) > 1 or 
        (len(documents) == 1 and documents[0].metadata.get("H1"))
    )
    
    if not has_meaningful_h1_splits:
        # Fall back to H2 headings if no H1s found
        header_splitter = MarkdownHeaderTextSplitter(
            headers_to_split_on=[
                ("##", "H2"),
            ]
        )
        h2_documents = header_splitter.split_text(markdown_content)
        
        has_meaningful_h2_splits = (
            len(h2_documents) > 1 or 
            (len(h2_documents) == 1 and h2_documents[0].metadata.get("H2"))
        )
        
        if has_meaningful_h2_splits:
            documents = h2_documents
        else:
            # No meaningful headings found, use fixed-size chunking
            return fixed_size_chunking(markdown_content, chunk_size, chunk_overlap)

    chunks = []
    for doc in documents:
        section_text = doc.page_content.strip()
        metadata = doc.metadata or {}

        # Determine heading and heading line
        if metadata.get("H1"):
            heading = metadata["H1"]
            heading_line = f"# {heading}"
        elif metadata.get("H2"):
            heading = metadata["H2"]
            heading_line = f"## {heading}"
        else:
            heading = ""
            heading_line = ""

        full_content = f"{heading_line}\n\n{section_text}" if heading_line else section_text
        
        if count_tokens(full_content) <= chunk_size:
            # Entire section fits in one chunk
            chunks.append({
                "text": full_content,
                "heading": heading,
            })
        else:
            # Section needs to be split with continuation markers
            if heading_line:
                continuation_overhead = count_tokens(f"{heading_line} (continued)\n\n")
            else:
                continuation_overhead = count_tokens("(continued)\n\n")
            
            available_size = chunk_size - continuation_overhead
            
            text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
                chunk_size=available_size,
                chunk_overlap=chunk_overlap
            )
            
            sub_chunks = text_splitter.split_text(section_text)
            
            for i, sub_chunk in enumerate(sub_chunks):
                sub_chunk = sub_chunk.strip()
                
                # Smart thresholding: check if we should merge with next chunk
                if i < len(sub_chunks) - 1:
                    current_chunk_tokens = count_tokens(sub_chunk)
                    next_chunk_tokens = count_tokens(sub_chunks[i + 1])
                    
                    if (current_chunk_tokens + next_chunk_tokens + continuation_overhead <= chunk_size and 
                        next_chunk_tokens < threshold):
                        # Merge with next chunk
                        merged_content = f"{sub_chunk}\n\n{sub_chunks[i + 1].strip()}"
                        sub_chunks[i + 1] = merged_content
                        continue
                
                # Create the chunk with appropriate heading
                if i == 0:
                    combined_text = f"{heading_line}\n\n{sub_chunk}" if heading_line else sub_chunk
                else:
                    if heading_line:
                        continuation_marker = f"{heading_line} (continued)\n\n{sub_chunk}"
                    else:
                        continuation_marker = f"(continued)\n\n{sub_chunk}"
                    combined_text = continuation_marker
                
                chunks.append({
                    "text": combined_text,
                    "heading": heading,
                })

    return chunks

def h2_heading_based_chunking(markdown_content, chunk_size=750, chunk_overlap=100):
    """
    H2 heading-based chunking with continuation markers:
    - Split on H1 (#) and H2 (##) headings
    - When content exceeds token limit, split with continuation markers
    - Each chunk maintains heading context for better understanding
    """
    header_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=[
            ("#", "H1"),
            ("##", "H2"),
        ]
    )
    documents = header_splitter.split_text(markdown_content)

    chunks = []
    for doc in documents:
        section_text = doc.page_content.strip()
        metadata = doc.metadata or {}

        # Determine the most specific heading (H2 over H1)
        heading = None
        if metadata.get("H2"):
            heading = metadata["H2"]
        elif metadata.get("H1"):
            heading = metadata["H1"]

        heading_line = f"## {heading}" if heading else ""

        full_content = f"{heading_line}\n\n{section_text}" if heading_line else section_text
        
        if count_tokens(full_content) <= chunk_size:
            chunks.append({
                "text": full_content,
                "heading": heading,
            })
        else:
            # Section needs to be split with continuation markers
            text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
                chunk_size=chunk_size - count_tokens(heading_line) - 50,
                chunk_overlap=chunk_overlap
            )
            
            sub_chunks = text_splitter.split_text(section_text)
            
            for i, sub_chunk in enumerate(sub_chunks):
                sub_chunk = sub_chunk.strip()
                
                if i == 0:
                    combined_text = f"{heading_line}\n\n{sub_chunk}" if heading_line else sub_chunk
                else:
                    continuation_marker = f"{heading_line} (continued)\n\n{sub_chunk}" if heading_line else f"(continued)\n\n{sub_chunk}"
                    combined_text = continuation_marker
                
                chunks.append({
                    "text": combined_text,
                    "heading": heading,
                })

    return chunks

def get_html_chunks(html_doc, chunk_size=750, chunk_overlap=100, 
                   chunking_strategy=CHUNKING_STRATEGY_FIXED,
                   extract_main_content=False):
    """
    Process HTML document and return chunks based on specified strategy.
    
    Args:
        html_doc: Document dictionary with file_data and metadata_storage_path
        chunk_size: Maximum tokens per chunk (default: 750)
        chunk_overlap: Token overlap between chunks (default: 100)
        chunking_strategy: Strategy to use - one of:
            - "fixed_size": Fixed-size chunking (default)
            - "h1_heading_based": H1-based with fallback to H2/fixed
            - "h2_heading_based": H1+H2 hierarchical chunking
        extract_main_content: If True, extract only the main content using Trafilatura (default: False)

    Returns:
        List of chunk dictionaries with url, chunk text, and position
    """
    file_path = html_doc["metadata_storage_path"]
    logger.info(f"Processing html document: [{file_path.split('/')[-1]}] with strategy: {chunking_strategy}")

    # Extract and convert to markdown
    html_content = get_html_content(html_doc)
    clean_html = extract_html_main_content(html_content, extract_main_content=extract_main_content)
    markdown_content = html_to_markdown(clean_html)

    # Apply chunking strategy
    if chunking_strategy == CHUNKING_STRATEGY_H1_BASED:
        chunks = h1_heading_based_chunking(markdown_content, chunk_size, chunk_overlap)
    elif chunking_strategy == CHUNKING_STRATEGY_H2_BASED:
        chunks = h2_heading_based_chunking(markdown_content, chunk_size, chunk_overlap)
    else:
        chunks = fixed_size_chunking(markdown_content, chunk_size, chunk_overlap)

    # Build chunk list with URLs
    chunks_list = []
    for i, chunk_data in enumerate(chunks):
        chunk_text = chunk_data["text"]
        metadata_storage_path = html_doc["metadata_storage_path"]
        
        # URL construction logic
        is_user_doc = ('User Documentation' in metadata_storage_path) or ('User%20Documentation' in metadata_storage_path)
        is_tech_doc = ('Technical Documentation' in metadata_storage_path) or ('Technical%20Documentation' in metadata_storage_path)
        is_ale_doc = ('aledoc25r1' in metadata_storage_path) or ('ale%20doc25r1' in metadata_storage_path)
        
        if is_tech_doc:
            path_parts = metadata_storage_path.split('/techdocs25r1')[-1] if '/techdocs25r1' in metadata_storage_path else metadata_storage_path.lstrip('/')
            relative_path = path_parts
            url = f"https://docs.ifs.com/techdocs/25r1{relative_path}"
        elif is_user_doc:
            path_parts = metadata_storage_path.split('/en/')[-1] if '/en/' in metadata_storage_path else metadata_storage_path.lstrip('/')
            relative_path = path_parts
            url = f"https://docs.ifs.com/ifsclouddocs/25r1/{relative_path}"
        elif is_ale_doc:
            path_parts = metadata_storage_path.split('/aledoc25r1')[-1]
            relative_path = path_parts
            url = f"https://docs.ifs.com/techdocs/ale/{relative_path}"
        else:
            url = metadata_storage_path
        chunks_list.append({"url": url, "chunk": chunk_text, "position": i})
    logger.info(f"Processing html document: [{file_path.split('/')[-1]}] completed with {len(chunks_list)} chunks")
    return chunks_list