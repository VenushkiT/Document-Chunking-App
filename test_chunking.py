import unittest
import base64
from chunking import (
    get_html_content,
    html_to_markdown,
    get_metadata_paths,
    get_html_title,
    get_html_chunks,
    fixed_size_chunking,
    h1_heading_based_chunking,
    h2_heading_based_chunking,
    extract_html_main_content,
    count_tokens,
    CHUNKING_STRATEGY_FIXED,
    CHUNKING_STRATEGY_H1_BASED,
    CHUNKING_STRATEGY_H2_BASED
)

class TestChunking(unittest.TestCase):

    def setUp(self):
        # Minimal HTML sample with title and meta tags
        self.html_with_meta = """
            <html>
                <head>
                    <title>Test Document</title>
                    <meta name="category" content='"cat1", "cat2", "cat3"'/>
                </head>
                <body>
                    <h1>Heading 1</h1>
                    <p>This is a paragraph.</p>
                    <h2>Heading 2</h2>
                    <p>Another paragraph.</p>
                    <nav>Navigation content</nav>
                </body>
            </html>
        """
        self.encoded_html = base64.b64encode(self.html_with_meta.encode("utf-8")).decode("utf-8")

        self.html_doc = {
            "file_data": {"data": self.encoded_html},
            "metadata_storage_path": "User%20Documentation/en/somefile.html"
        }

        self.plain_markdown = """# Heading 1

This is a paragraph.

## Heading 2

Another paragraph."""

    def test_get_html_content_utf8(self):
        content = get_html_content(self.html_doc)
        self.assertIn("<h1>Heading 1</h1>", content)

    def test_html_to_markdown(self):
        md_output = html_to_markdown(self.html_with_meta)
        self.assertTrue(
            "# Heading 1" in md_output or "Heading 1\n=========" in md_output
        )
        self.assertIn("This is a paragraph.", md_output)

    def test_get_metadata_paths(self):
        html_content = base64.b64decode(self.html_doc["file_data"]["data"]).decode("utf-8")
        paths = get_metadata_paths(html_content, "path/to/storage")
        self.assertListEqual(paths, ["cat1", "cat2", "cat3"])

    def test_get_html_title(self):
        html_content = base64.b64decode(self.html_doc["file_data"]["data"]).decode("utf-8")
        title = get_html_title(html_content, "path/to/storage")
        self.assertEqual(title, "Test Document")

    def test_get_html_title_no_title(self):
        html_no_title = "<html><body>Content</body></html>"
        title = get_html_title(html_no_title, "path/to/storage")
        self.assertEqual(title, "Untitled")

    def test_count_tokens(self):
        text = "This is a test sentence."
        token_count = count_tokens(text)
        self.assertGreater(token_count, 0)

    def test_extract_html_main_content(self):
        # Test with extract_main_content=True
        extracted = extract_html_main_content(self.html_with_meta, extract_main_content=True)
        # Test that main content paragraphs are preserved
        self.assertIn("This is a paragraph.", extracted)
        self.assertIn("Another paragraph.", extracted)
        # Test that navigation is removed
        self.assertNotIn("Navigation content", extracted)

        # Test with extract_main_content=False
        not_extracted = extract_html_main_content(self.html_with_meta, extract_main_content=False)
        self.assertEqual(not_extracted, self.html_with_meta)

    def test_heading_preservation_in_chunks(self):
        # First convert HTML to markdown
        markdown = html_to_markdown(self.html_with_meta)
        self.assertIn("# Heading 1", markdown)
        self.assertIn("## Heading 2", markdown)

        # Then test chunking with explicit debug output
        chunks = get_html_chunks(
            self.html_doc,
            chunk_size=1000,  # Much larger chunk size
            chunk_overlap=50,
            chunking_strategy=CHUNKING_STRATEGY_FIXED,
            extract_main_content=False
        )
        
        # Debug print the chunks
        chunk_texts = [chunk["chunk"] for chunk in chunks]
        print("\nDebug: Checking chunks for headings:")
        for i, text in enumerate(chunk_texts):
            print(f"\nChunk {i}:")
            print(text[:200] + "..." if len(text) > 200 else text)
            
        # Check for heading
        heading_preserved = any("# Heading 1" in text for text in chunk_texts)
        if not heading_preserved:
            print("\nWarning: No chunks contained '# Heading 1'. Available chunks:")
            for i, text in enumerate(chunk_texts):
                print(f"\nChunk {i} preview:")
                print(text[:100])
                
        self.assertTrue(heading_preserved, "Heading '# Heading 1' not found in any chunk")

    def test_fixed_size_chunking(self):
        chunks = fixed_size_chunking(self.plain_markdown, chunk_size=50, chunk_overlap=10)
        self.assertTrue(len(chunks) > 0)
        for chunk in chunks:
            self.assertIn("text", chunk)
            self.assertIn("heading", chunk)

    def test_h1_heading_based_chunking(self):
        chunks = h1_heading_based_chunking(self.plain_markdown, chunk_size=100, chunk_overlap=10)
        self.assertTrue(len(chunks) > 0)
        self.assertTrue(any("# Heading 1" in chunk["text"] for chunk in chunks))
        self.assertTrue(any("Heading 1" in chunk["heading"] for chunk in chunks))

    def test_h2_heading_based_chunking(self):
        chunks = h2_heading_based_chunking(self.plain_markdown, chunk_size=100, chunk_overlap=10)
        self.assertTrue(len(chunks) > 0)
        self.assertTrue(any("## Heading 2" in chunk["text"] for chunk in chunks))

    def test_get_html_chunks_with_different_strategies(self):
        # Test fixed size strategy
        fixed_chunks = get_html_chunks(
            self.html_doc, 
            chunk_size=100, 
            chunk_overlap=10,
            chunking_strategy=CHUNKING_STRATEGY_FIXED
        )
        self.assertTrue(len(fixed_chunks) > 0)

        # Test H1-based strategy
        h1_chunks = get_html_chunks(
            self.html_doc,
            chunk_size=100,
            chunk_overlap=10,
            chunking_strategy=CHUNKING_STRATEGY_H1_BASED
        )
        self.assertTrue(len(h1_chunks) > 0)

        # Test H2-based strategy
        h2_chunks = get_html_chunks(
            self.html_doc,
            chunk_size=100,
            chunk_overlap=10,
            chunking_strategy=CHUNKING_STRATEGY_H2_BASED
        )
        self.assertTrue(len(h2_chunks) > 0)

    def test_get_html_chunks_url_generation(self):
        # Test User Documentation URL
        user_doc = {
            "file_data": {"data": self.encoded_html},
            "metadata_storage_path": "User Documentation/en/somefile.html"
        }
        chunks = get_html_chunks(user_doc)
        self.assertTrue(any("https://docs.ifs.com/ifsclouddocs/25r1/" in chunk["url"] for chunk in chunks))

        # Test Technical Documentation URL
        tech_doc = {
            "file_data": {"data": self.encoded_html},
            "metadata_storage_path": "Technical Documentation/techdocs25r1/somefile.html"
        }
        chunks = get_html_chunks(tech_doc)
        self.assertTrue(any("https://docs.ifs.com/techdocs/25r1" in chunk["url"] for chunk in chunks))

        # Test ALE Documentation URL
        ale_doc = {
            "file_data": {"data": self.encoded_html},
            "metadata_storage_path": "aledoc25r1/somefile.html"
        }
        chunks = get_html_chunks(ale_doc)
        self.assertTrue(any("https://docs.ifs.com/techdocs/ale/" in chunk["url"] for chunk in chunks))

if __name__ == "__main__":
    unittest.main()