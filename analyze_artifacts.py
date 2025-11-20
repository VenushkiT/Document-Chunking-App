"""
Artifact Analysis Script
Analyzes JSON artifacts for chunk token/character statistics and visualizations.
"""
import json
import argparse
from pathlib import Path
from datetime import datetime
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import tiktoken

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class ArtifactAnalyzer:
    def __init__(self, manifest_path):
        self.manifest_path = Path(manifest_path)
        self.artifacts = []
        self.report_data = {}
        self.df = None
        try:
            self.tokenizer = tiktoken.get_encoding("cl100k_base")
        except Exception:
            self.tokenizer = None

    def load_artifacts(self):
        """Load artifacts from JSONL manifest"""
        if not self.manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found: {self.manifest_path}")

        with open(self.manifest_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    artifact = json.loads(line)
                    self.artifacts.append(artifact)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse line {line_num}: {e}")

        logger.info(f"Loaded {len(self.artifacts)} artifacts")
        return len(self.artifacts)

    def count_tokens(self, text):
        """Count tokens using tiktoken"""
        if self.tokenizer is None:
            return None
        try:
            return len(self.tokenizer.encode(text))
        except Exception as e:
            logger.debug(f"Token count failed: {e}")
            return None

    def analyze(self):
        """Analyze chunks: characters and tokens"""
        if not self.artifacts:
            logger.warning("No artifacts to analyze")
            return {}

        chunk_data = []
        for idx, artifact in enumerate(self.artifacts):
            chunk_text = artifact.get('chunk', '')
            if chunk_text:
                char_count = len(chunk_text)
                token_count = self.count_tokens(chunk_text)
                chunk_data.append({
                    'chunk_id': artifact.get('chunk_id', f'chunk_{idx}'),
                    'character_count': char_count,
                    'token_count': token_count
                })

        if chunk_data:
            self.df = pd.DataFrame(chunk_data)

        # Simple statistics
        char_counts = [d['character_count'] for d in chunk_data]
        token_counts = [d['token_count'] for d in chunk_data if d['token_count'] is not None]

        if char_counts:
            self.report_data['chunk_character_stats'] = {
                'min': min(char_counts),
                'max': max(char_counts),
                'mean': sum(char_counts)/len(char_counts),
                'median': sorted(char_counts)[len(char_counts)//2]
            }

        if token_counts:
            self.report_data['chunk_token_stats'] = {
                'min': min(token_counts),
                'max': max(token_counts),
                'mean': sum(token_counts)/len(token_counts),
                'median': sorted(token_counts)[len(token_counts)//2]
            }

        return self.report_data

    def generate_descriptive_statistics(self):
        """Descriptive stats table for chunks"""
        if self.df is None:
            logger.warning("No DataFrame available for descriptive statistics")
            return None

        numeric_cols = ['character_count']
        if 'token_count' in self.df.columns and self.df['token_count'].notna().any():
            numeric_cols.append('token_count')

        desc_stats = self.df[numeric_cols].describe(percentiles=[0.05,0.25,0.5,0.75,0.95])
        return desc_stats

    def generate_visualizations(self, output_dir):
        """Generate plots showing distribution of character and token counts per chunk"""
        if self.df is None:
            logger.warning("No DataFrame available for visualization")
            return []

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        plot_files = []

        sns.set_style("whitegrid")

        # Character count plots
        try:
            fig, axes = plt.subplots(1, 2, figsize=(12, 5))
            # Histogram
            data_chars = self.df['character_count'].dropna()
            axes[0].hist(data_chars, bins=50, color='lightgreen', edgecolor='black', alpha=0.7, density=True)
            axes[0].set_title("Character Count Distribution (Histogram)")
            axes[0].set_xlabel("Characters")
            axes[0].set_ylabel("Density")
            # Overlay seaborn KDE
            if data_chars.size > 1:
                try:
                    sns.kdeplot(data=data_chars, ax=axes[0], color='darkgreen', lw=2, label='KDE')
                    axes[0].legend()
                except Exception as e:
                    logger.debug(f"KDE plot failed for character counts: {e}")
            # Boxplot
            axes[1].boxplot(self.df['character_count'].dropna(), vert=True, patch_artist=True,
                            boxprops=dict(facecolor='lightcoral', color='red'),
                            medianprops=dict(color='black', linewidth=2))
            axes[1].set_title("Character Count Boxplot")
            axes[1].set_ylabel("Characters")

            plt.tight_layout()
            char_plot_file = output_dir / f"character_distribution_{timestamp}.png"
            plt.savefig(char_plot_file, dpi=300, bbox_inches='tight')
            plt.close(fig)
            plot_files.append(str(char_plot_file))
            logger.info(f"Saved character distribution plot: {char_plot_file}")
        except Exception as e:
            logger.error(f"Failed to generate character plots: {e}")

        # Token count plots (if available)
        if 'token_count' in self.df.columns and self.df['token_count'].notna().any():
            try:
                fig, axes = plt.subplots(1, 2, figsize=(12, 5))
                scatter_data = self.df['token_count'].dropna()
                # Histogram
                axes[0].hist(scatter_data, bins=50, color='orange', edgecolor='black', alpha=0.7, density=True)
                axes[0].set_title("Token Count Distribution (Histogram)")
                axes[0].set_xlabel("Tokens")
                axes[0].set_ylabel("Density")
                # Overlay seaborn KDE
                if scatter_data.size > 1:
                    try:
                        sns.kdeplot(data=scatter_data, ax=axes[0], color='darkred', lw=2, label='KDE')
                        axes[0].legend()
                    except Exception as e:
                        logger.debug(f"KDE plot failed for token counts: {e}")
                # Boxplot
                axes[1].boxplot(scatter_data, vert=True, patch_artist=True,
                                boxprops=dict(facecolor='lightblue', color='navy'),
                                medianprops=dict(color='red', linewidth=2))
                axes[1].set_title("Token Count Boxplot")
                axes[1].set_ylabel("Tokens")

                plt.tight_layout()
                token_plot_file = output_dir / f"token_distribution_{timestamp}.png"
                plt.savefig(token_plot_file, dpi=300, bbox_inches='tight')
                plt.close(fig)
                plot_files.append(str(token_plot_file))
                logger.info(f"Saved token distribution plot: {token_plot_file}")
            except Exception as e:
                logger.error(f"Failed to generate token plots: {e}")

        return plot_files

    def generate_report(self, output_dir=None):
        """Generate text report + visualizations"""
        if not self.report_data:
            logger.warning("No analysis data. Run analyze() first.")
            return None

        if output_dir is None:
            output_dir = self.manifest_path.parent / "reports"
        else:
            output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        plot_files = self.generate_visualizations(output_dir)
        desc_stats = self.generate_descriptive_statistics()

        txt_report_path = output_dir / f"analysis_report_{timestamp}.txt"
        with open(txt_report_path, 'w', encoding='utf-8') as f:
            f.write("ARTIFACT CHUNK ANALYSIS REPORT\n")
            f.write(f"Total Artifacts: {len(self.artifacts)}\n\n")

            if desc_stats is not None:
                f.write("DESCRIPTIVE STATISTICS\n")
                f.write(desc_stats.to_string())
                f.write("\n\n")

            f.write("CHUNK STATISTICS\n")
            if 'chunk_character_stats' in self.report_data:
                cs = self.report_data['chunk_character_stats']
                f.write(f"Chars - min:{cs['min']} max:{cs['max']} mean:{cs['mean']:.2f} median:{cs['median']}\n")
            if 'chunk_token_stats' in self.report_data:
                ts = self.report_data['chunk_token_stats']
                f.write(f"Tokens - min:{ts['min']} max:{ts['max']} mean:{ts['mean']:.2f} median:{ts['median']}\n")

            if plot_files:
                f.write("\nVISUALIZATIONS\n")
                for p in plot_files:
                    f.write(f" • {Path(p).name}\n")

        return {'txt_report': str(txt_report_path), 'plot_files': plot_files}


def find_latest_manifest(manifests_root="./manifests"):
    manifests_path = Path(manifests_root)
    if not manifests_path.exists():
        return None
    manifest_files = list(manifests_path.rglob("manifest_*.jsonl"))
    if not manifest_files:
        return None
    return max(manifest_files, key=lambda p: p.stat().st_mtime)


def main():
    parser = argparse.ArgumentParser(description="Analyze artifact chunks")
    parser.add_argument('--manifest', type=str)
    parser.add_argument('--latest', action='store_true')
    parser.add_argument('--output', type=str)
    args = parser.parse_args()

    if args.latest:
        manifest_path = find_latest_manifest()
        if not manifest_path:
            logger.error("No manifest found")
            return 1
    elif args.manifest:
        manifest_path = args.manifest
    else:
        logger.error("Specify --manifest or --latest")
        return 1

    analyzer = ArtifactAnalyzer(manifest_path)
    count = analyzer.load_artifacts()
    if count == 0:
        logger.error("No artifacts to analyze")
        return 1

    analyzer.analyze()
    reports = analyzer.generate_report(output_dir=args.output)
    if reports:
        print(f"Text Report: {reports['txt_report']}")
        if reports.get('plot_files'):
            print(f"Plots: {len(reports['plot_files'])} files")
            for p in reports['plot_files']:
                print(f"  • {Path(p).name}")
    return 0


if __name__ == "__main__":
    exit(main())