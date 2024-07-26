import json

# JSON data

# Parse the JSON data
with open('data.json', 'r', encoding='utf-8') as file:
    json_objects = json.load(file)


# Function to generate markdown report with tables
def generate_markdown_report(data):
    markdown = '# Expectation Results Report\n\n'

    # Header for the table
    markdown += '| Column | Test Name                           | Result  | Statistics |\n'
    markdown += '| ------- | ----------------------------------- | ------- | ------------ |\n'

    for expectation in data:
        test_name = expectation["test"]
        result = "Success" if expectation["success"] else "Failure"
        result_color = "#27ae60" if expectation["success"] else "#c0392b"
        config = json.dumps(expectation["kwargs"], indent=2).replace('\n', ' ').replace('  ', ' ')
        result_data = expectation["result"]
        element_count = result_data.get("element_count", 0)
        missing_count = result_data.get("missing_count", 0)
        unexpected_count = result_data.get("unexpected_count", 0)
        unexpected_percent = result_data.get("unexpected_percent", 0.0)
        missing_percent = result_data.get("missing_percent", 0.0)
        unexpected_percent_total = result_data.get("unexpected_percent_total", 0.0)
        unexpected_percent_nonmissing = result_data.get("unexpected_percent_nonmissing", 0.0)

        statistics_var = (f'Element Count: {element_count}, '
                          f'Missing Count: {missing_count}, '
                          f'Missing Percent: {missing_percent:.2f}, '
                          f'Unexpected Count: {unexpected_count}, '
                          f'Unexpected Percent: {unexpected_percent:.2f}, '
                          f'Unexpected Percent Total: {unexpected_percent_total:.2f}, '
                          f'Unexpected Percent Nonmissing: {unexpected_percent_nonmissing:.2f}')
        if not expectation["kwargs"]["column"]:
            print('a')
        # Add each row to the table with HTML for cell coloring
        markdown += (f'| <span style="color: {result_color};">{expectation["kwargs"]["column"]}</span> | '
                    f' <span style="color: {result_color};">{test_name}</span> | '
                     f'<span style="color: {result_color};">{result}</span> | '
                     f'```{statistics_var}``` |\n')

    return markdown

# Generate the markdown report
markdown_report = generate_markdown_report(json_objects)

# Save the Markdown report to a file
with open('expectations_report.md', 'w', encoding='utf-8') as f:
    f.write(markdown_report)

print("Markdown report has been generated as 'expectations_report.md'")
