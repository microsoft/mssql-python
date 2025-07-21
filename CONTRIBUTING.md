# Contributing to mssql-python

This project welcomes contributions and suggestions. Most contributions require you to
agree to a Contributor License Agreement (CLA) declaring that you have the right to,
and actually do, grant us the rights to use your contribution. For details, visit
https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need
to provide a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the
instructions provided by the bot. You will only need to do this once across all repositories using our CLA.

## Getting Started

### For External Contributors

If you're an external contributor (not a Microsoft organization member), please follow these steps:

1. **Create a GitHub Issue**: Before submitting a pull request, please create a GitHub issue describing the bug, feature request, or improvement you'd like to make.
2. **Reference the Issue**: In your pull request description, include a link to the GitHub issue in this format:
   ```
   Related to: https://github.com/microsoft/mssql-python/issues/XXX
   ```
3. **Follow PR Guidelines**: Ensure your PR title follows the required format (see below) and includes a meaningful summary.

### For Internal Contributors (Microsoft Organization Members)

If you're a Microsoft organization member:

1. **Create an ADO Work Item**: Ensure you have a corresponding Azure DevOps work item for your changes.
2. **Reference the Work Item**: In your pull request description, include a link to the ADO work item in this format:
   ```
   AB#<WORK_ITEM_ID>
   ```
3. **Follow PR Guidelines**: Ensure your PR title follows the required format (see below) and includes a meaningful summary.

## Pull Request Guidelines

### PR Title Format

Your PR title must start with one of the following prefixes:

- `FEAT:` - For new features
- `FIX:` - For bug fixes  
- `CHORE:` - For maintenance tasks, test updates, config updates, dependency updates, etc.
- `DOC:` - For documentation updates
- `STYLE:` - For formatting, indentation, or styling updates
- `REFACTOR:` - For code refactoring without feature changes
- `RELEASE:` - For release-related changes

Example: `FEAT: Add connection pooling support`

### PR Description Requirements

- **Summary**: Include a meaningful summary of your changes (minimum 10 characters)
- **Issue/Work Item Reference**: Link to either a GitHub issue (external contributors) or ADO work item (internal contributors)
- **Testing**: Describe how you tested your changes

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.