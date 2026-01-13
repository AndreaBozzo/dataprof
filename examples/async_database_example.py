#!/usr/bin/env python3
"""
Example demonstrating async database profiling with dataprof

This example shows how to use the async Python bindings to profile
database queries without blocking, using Python's asyncio framework.

Prerequisites:
- Build dataprof with async features:
  maturin develop --features python-async,database,sqlite

- For PostgreSQL/MySQL, ensure database is running and accessible
"""

import asyncio
import dataprof


async def example_sqlite_async():
    """
    Example: Profile an in-memory SQLite database asynchronously
    """
    print("📊 SQLite Async Example")
    print("=" * 50)

    # Use in-memory SQLite database (no setup required)
    connection_string = "sqlite::memory:"

    try:
        # Test connection
        print("\n1️⃣ Testing connection...")
        is_connected = await dataprof.test_connection_async(connection_string)
        print(f"   Connected: {is_connected}")

        # Note: In a real scenario, you would create tables and insert data
        # For this example, we'll show the API usage

        print("\n✅ SQLite async example completed!")

    except Exception as e:
        print(f"❌ Error: {e}")


async def example_postgres_async():
    """
    Example: Profile a PostgreSQL database asynchronously

    Note: Requires a running PostgreSQL instance
    """
    print("\n📊 PostgreSQL Async Example")
    print("=" * 50)

    # Update this connection string to match your setup
    connection_string = "postgresql://user:password@localhost/testdb"

    try:
        # Test connection
        print("\n1️⃣ Testing connection...")
        is_connected = await dataprof.test_connection_async(connection_string)

        if not is_connected:
            print("   ⚠️  Connection failed. Check your connection string.")
            return

        print("   ✓ Connected!")

        # Get table schema
        print("\n2️⃣ Getting table schema...")
        table_name = "users"
        try:
            columns = await dataprof.get_table_schema_async(
                connection_string,
                table_name
            )
            print(f"   Table '{table_name}' has {len(columns)} columns:")
            for col in columns:
                print(f"   - {col}")
        except Exception as e:
            print(f"   ⚠️  Could not get schema: {e}")

        # Count rows
        print("\n3️⃣ Counting rows...")
        try:
            row_count = await dataprof.count_table_rows_async(
                connection_string,
                table_name
            )
            print(f"   Table '{table_name}' has {row_count:,} rows")
        except Exception as e:
            print(f"   ⚠️  Could not count rows: {e}")

        # Profile a query
        print("\n4️⃣ Profiling query...")
        query = f"SELECT * FROM {table_name} LIMIT 1000"

        result = await dataprof.analyze_database_async(
            connection_string,
            query,
            batch_size=1000,
            calculate_quality=True
        )

        print(f"   Profiled {result['row_count']} rows")
        print(f"   Found {len(result['columns'])} columns")

        # Show column statistics
        for col_name, profile in result['columns'].items():
            print(f"\n   Column: {col_name}")
            print(f"   - Type: {profile.data_type}")
            print(f"   - Unique: {profile.unique_count}")
            print(f"   - Nulls: {profile.null_count}")

        # Show quality metrics if available
        if 'quality' in result:
            quality = result['quality']
            print(f"\n   Quality Score: {quality.overall_score:.2%}")
            print(f"   - Completeness: {quality.completeness_score:.2%}")
            print(f"   - Validity: {quality.validity_score:.2%}")
            print(f"   - Consistency: {quality.consistency_score:.2%}")

        print("\n✅ PostgreSQL async example completed!")

    except Exception as e:
        print(f"❌ Error: {e}")


async def example_concurrent_queries():
    """
    Example: Run multiple async queries concurrently

    This demonstrates the power of async - you can profile multiple
    queries or databases at the same time without blocking.
    """
    print("\n📊 Concurrent Queries Example")
    print("=" * 50)

    # Define multiple connection strings (update to match your setup)
    connections = [
        ("sqlite::memory:", "SELECT 1"),
        ("sqlite::memory:", "SELECT 2"),
        ("sqlite::memory:", "SELECT 3"),
    ]

    print(f"\n🚀 Running {len(connections)} queries concurrently...")

    # Create tasks for all queries
    tasks = [
        dataprof.test_connection_async(conn)
        for conn, _ in connections
    ]

    # Run all tasks concurrently
    start_time = asyncio.get_event_loop().time()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    end_time = asyncio.get_event_loop().time()

    # Show results
    successful = sum(1 for r in results if r is True)
    print(f"   ✓ {successful}/{len(connections)} queries successful")
    print(f"   ⏱️  Total time: {end_time - start_time:.2f}s")

    print("\n✅ Concurrent queries example completed!")


async def example_streaming_profile():
    """
    Example: Profile a large dataset using streaming

    This shows how async + streaming enables efficient processing
    of large result sets without consuming too much memory.
    """
    print("\n📊 Streaming Profile Example")
    print("=" * 50)

    connection_string = "sqlite::memory:"

    # For large queries, use smaller batch sizes to stream results
    query = "SELECT * FROM large_table"  # Would need actual table

    print("\n📦 Using batch processing for large datasets...")
    print("   Batch size: 5000 rows")
    print("   This processes data in chunks to avoid memory issues")

    # Note: In a real scenario with actual data, this would demonstrate
    # how async + streaming handles large datasets efficiently

    print("\n💡 Tip: Combine async with streaming for best performance")
    print("   - Async: Non-blocking I/O")
    print("   - Streaming: Memory-efficient processing")

    print("\n✅ Streaming example completed!")


async def main():
    """
    Main entry point - runs all examples
    """
    print("🐍 DataProf Async Python Examples")
    print("=" * 50)
    print("\nThese examples demonstrate async database profiling")
    print("using Python's asyncio framework.\n")

    # Run examples
    await example_sqlite_async()

    # Uncomment to run PostgreSQL example (requires setup)
    # await example_postgres_async()

    await example_concurrent_queries()
    await example_streaming_profile()

    print("\n" + "=" * 50)
    print("✨ All async examples completed!")
    print("\n📚 Learn more:")
    print("   - Use --features python-async,database,postgres for PostgreSQL")
    print("   - Use --features python-async,database,mysql for MySQL")
    print("   - Combine with streaming for large datasets")


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
