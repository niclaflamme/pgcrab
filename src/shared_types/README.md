# Shared Types

This folder houses types that are referenced by multiple modules without a clear
single owner. Keeping them here avoids circular dependencies and keeps imports
simple across the codebase.

It is also intentionally easy to find, so LLMs and contributors can quickly
locate common types when navigating or extending the project.

Types with clear ownership boundaries should still live alongside their
respective modules rather than being placed here.
