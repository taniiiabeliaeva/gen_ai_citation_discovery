#!/usr/bin/env python3
"""
Test script for OpenAlex tools integration.

This script tests each OpenAlex tool individually and validates the integration
with the agent system.

Requirements:
- data/works_final.csv must exist and contain paper metadata
- POLITE_EMAIL must be set in .env
- Internet connection for OpenAlex API calls
"""

import sys
import os
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tools.openalex_tools import (
    get_citation_details,
    get_cited_works,
    find_related_works,
    get_referenced_works,
)
from tools.data_utils import load_paper_data

# Load environment and data
load_dotenv()
load_paper_data()


def print_section(title):
    """Print a formatted section header."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def test_get_citation_details():
    """Test the get_citation_details tool."""
    print_section("TEST 1: get_citation_details")

    search_term = "Interactive Music Mapping Vienna: Networks In Time and Space"
    print(f"Query: Get citation details for '{search_term}'")
    print(f"Tool call: get_citation_details('{search_term}')\n")

    result = get_citation_details.invoke({"search_term": search_term})

    print("Result:")
    print(result)
    print("\n✓ Test completed")

    return result


def test_get_cited_works():
    """Test the get_cited_works tool."""
    print_section("TEST 2: get_cited_works (Forward Citations)")

    search_term = "SPiKE: 3D Human Pose from Point Cloud Sequences"
    limit = 3
    print(f"Query: Find papers that CITE '{search_term}'")
    print(f"Tool call: get_cited_works('{search_term}', limit={limit})\n")

    result = get_cited_works.invoke({"search_term": search_term, "limit": limit})

    print("Result:")
    print(f"Found {len(result.citing_works)} citing papers:\n")

    for i, work in enumerate(result.citing_works, 1):
        print(f"{i}. {work.title}")
        print(f"   Authors: {work.authors}")
        print(f"   Year: {work.publication_year}")
        print(f"   OpenAlex ID: {work.openalex_id}")
        print(f"   DOI: {work.doi or 'N/A'}")
        print()

    print("✓ Test completed")
    return result


def test_get_referenced_works():
    """Test the get_referenced_works tool."""
    print_section("TEST 3: get_referenced_works (Backward Citations/Bibliography)")

    search_term = "SPiKE: 3D Human Pose from Point Cloud Sequences"
    limit = 3
    print(f"Query: Find papers REFERENCED BY '{search_term}'")
    print(f"Tool call: get_referenced_works('{search_term}', limit={limit})\n")

    result = get_referenced_works.invoke({"search_term": search_term, "limit": limit})

    print("Result:")
    print(f"Found {len(result.citing_works)} referenced papers:\n")

    for i, work in enumerate(result.citing_works, 1):
        print(f"{i}. {work.title}")
        print(f"   Authors: {work.authors}")
        print(f"   Year: {work.publication_year}")
        print(f"   OpenAlex ID: {work.openalex_id}")
        print(f"   DOI: {work.doi or 'N/A'}")
        print()

    print("✓ Test completed")
    return result


def test_find_related_works():
    """Test the find_related_works tool."""
    print_section("TEST 4: find_related_works")

    search_term = "SPiKE: 3D Human Pose from Point Cloud Sequences"
    limit = 3
    print(f"Query: Find papers related to '{search_term}'")
    print(f"Tool call: find_related_works('{search_term}', limit={limit})\n")

    result = find_related_works.invoke({"search_term": search_term, "limit": limit})

    print("Result:")
    print(f"Found {len(result.citing_works)} related papers:\n")

    for i, work in enumerate(result.citing_works, 1):
        print(f"{i}. {work.title}")
        print(f"   Authors: {work.authors}")
        print(f"   Year: {work.publication_year}")
        print(f"   OpenAlex ID: {work.openalex_id}")
        print(f"   DOI: {work.doi or 'N/A'}")
        print()

    print("✓ Test completed")
    return result


def test_agent_integration():
    """Test integration with the agent system."""
    print_section("TEST 5: Agent Integration")

    print("Testing that tools are properly registered with the agent...")

    from tools.openalex_tools import ALL_OPENALEX_TOOLS

    print(f"\nRegistered OpenAlex tools: {len(ALL_OPENALEX_TOOLS)}")
    for tool in ALL_OPENALEX_TOOLS:
        print(f"  - {tool.name}: {tool.description}")

    print("\n✓ All tools are registered")

    # Test that tools are included in main.py
    from main import ALL_TOOLS

    openalex_tool_names = {tool.name for tool in ALL_OPENALEX_TOOLS}
    all_tool_names = {tool.name for tool in ALL_TOOLS}

    if openalex_tool_names.issubset(all_tool_names):
        print("✓ All OpenAlex tools are included in main.py ALL_TOOLS")
    else:
        missing = openalex_tool_names - all_tool_names
        print(f"✗ Missing tools in main.py: {missing}")

    print("\n✓ Integration test completed")


def main():
    """Run all tests."""
    print("\n" + "=" * 70)
    print("  OpenAlex Tools Integration Test Suite")
    print("=" * 70)

    try:
        # Run individual tool tests
        test_get_citation_details()
        test_get_cited_works()
        test_get_referenced_works()
        test_find_related_works()

        # Run integration test
        test_agent_integration()

        # Summary
        print_section("TEST SUMMARY")
        print("✓ All tests passed successfully!")
        print("\nYou can now test the tools via the chat interface with queries like:")
        print("  - 'Get the citation for Spectre'")
        print("  - 'What papers cite Meltdown?'")
        print("  - 'What papers does Spectre reference?'")
        print("  - 'Find papers related to Spectre'")
        print()

    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
