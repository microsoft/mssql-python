/**
 * Local test script for issue-triage + issue-notify workflows.
 * Tests the exact same logic: fetch issue → GitHub Models classification → Teams notification.
 *
 * Prerequisites: Node.js 18+ (for native fetch)
 *
 * Usage:
 *   $env:GH_TOKEN = "ghp_your_pat_here"   # needs models:read scope
 *   $env:TEAMS_WEBHOOK_URL = "https://your-webhook-url"
 *   node test-triage-local.js <issue_number>
 */

const REPO_OWNER = "microsoft";
const REPO_NAME = "mssql-python";

// --- Validate environment ---
const requiredEnv = ["GH_TOKEN", "TEAMS_WEBHOOK_URL"];
for (const key of requiredEnv) {
    if (!process.env[key]) {
        console.error(`ERROR: Missing environment variable: ${key}`);
        process.exit(1);
    }
}

const issueNumber = parseInt(process.argv[2]);
if (!issueNumber) {
    console.error("Usage: node test-triage-local.js <issue_number>");
    process.exit(1);
}

// --- Helper: GitHub Models ---
async function callGitHubModels(prompt) {
    const token = process.env.GH_TOKEN;
    const url = "https://models.github.ai/inference/chat/completions";

    const response = await fetch(url, {
        method: "POST",
        headers: {
            "Authorization": `Bearer ${token}`,
            "Content-Type": "application/json",
        },
        body: JSON.stringify({
            model: "openai/gpt-4.1",
            messages: [
                { role: "system", content: "You are an expert assistant. Always respond in valid json format." },
                { role: "user", content: prompt },
            ],
            temperature: 0.1,
            response_format: { type: "json_object" },
        }),
    });

    if (!response.ok) {
        const errText = await response.text();
        throw new Error(`GitHub Models error: ${response.status} - ${errText}`);
    }

    const data = await response.json();
    return data.choices[0].message.content;
}

// --- Helper: Fetch issue from GitHub ---
async function fetchIssue(issueNum) {
    const url = `https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/issues/${issueNum}`;
    const headers = { "Accept": "application/vnd.github.v3+json", "User-Agent": "triage-test" };

    // Use GH_TOKEN if available for higher rate limits
    if (process.env.GH_TOKEN) {
        headers["Authorization"] = `token ${process.env.GH_TOKEN}`;
    }

    const response = await fetch(url, { headers });
    if (!response.ok) {
        throw new Error(`GitHub API error: ${response.status} - ${await response.text()}`);
    }
    return response.json();
}

// --- Helper: Fetch file content from GitHub ---
async function fetchFileContent(filePath) {
    const url = `https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/contents/${filePath}`;
    const headers = { "Accept": "application/vnd.github.v3+json", "User-Agent": "triage-test" };

    if (process.env.GH_TOKEN) {
        headers["Authorization"] = `token ${process.env.GH_TOKEN}`;
    }

    const response = await fetch(url, { headers });
    if (!response.ok) {
        throw new Error(`Could not fetch ${filePath}: ${response.status}`);
    }
    const data = await response.json();
    return Buffer.from(data.content, "base64").toString();
}

// --- Helper: Send Teams notification ---
async function sendTeamsNotification(analysis, codeAnalysis, engineerGuidance, issue) {
    const category = analysis.category;
    const severity = analysis.severity;

    let emoji, categoryDisplay, action;
    switch (category) {
        case "FEATURE_REQUEST":
            emoji = "💡"; categoryDisplay = "Feature Request";
            action = "Evaluate against roadmap. If approved, create ADO work item.";
            break;
        case "BUG":
            emoji = "🐛"; categoryDisplay = "Bug";
            action = "Validate bug, reproduce if possible, assign to developer.";
            break;
        case "DISCUSSION":
            emoji = "💬"; categoryDisplay = "Discussion";
            action = "Respond with guidance. Re-classify if needed.";
            break;
        case "BREAK_FIX":
            emoji = "🚨"; categoryDisplay = "Break/Fix (Regression)";
            action = "URGENT: Assign to senior dev, create P0/P1 ADO item.";
            break;
        default:
            emoji = "❓"; categoryDisplay = "Unknown";
            action = "Review and manually classify this issue.";
    }

    const sevIndicator = severity === "critical" ? "🔴"
        : severity === "high" ? "🟠"
            : severity === "medium" ? "🟡" : "🟢";

    const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    let codeAnalysisText = "N/A — classification did not require code analysis.";
    let engineerGuidanceText = "";

    if (codeAnalysis) {
        try {
            const parsed = JSON.parse(codeAnalysis);

            // Format code analysis as HTML with bold labels
            const escVal = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
            const parts = [];
            if (parsed.is_bug) parts.push(`<b>Verdict:</b> ${escVal(parsed.is_bug)}`);
            if (parsed.root_cause) parts.push(`<b>Root Cause:</b> ${escVal(parsed.root_cause)}`);
            if (parsed.affected_components && parsed.affected_components.length > 0) {
                parts.push(`<b>Affected Components:</b><br>${parsed.affected_components.map(c => `&nbsp;&nbsp;• ${escVal(c)}`).join("<br>")}`);
            }
            if (parsed.evidence_and_context) parts.push(`<b>Evidence &amp; Context:</b> ${escVal(parsed.evidence_and_context)}`);
            if (parsed.recommended_fixes && parsed.recommended_fixes.length > 0) {
                parts.push(`<b>Recommended Fixes:</b><br>${parsed.recommended_fixes.map((s, i) => `&nbsp;&nbsp;${i + 1}. ${escVal(s)}`).join("<br>")}`);
            }
            if (parsed.code_locations && parsed.code_locations.length > 0) {
                parts.push(`<b>Code Locations:</b><br>${parsed.code_locations.map(l => `&nbsp;&nbsp;• ${escVal(l)}`).join("<br>")}`);
            }
            if (parsed.risk_assessment) parts.push(`<b>Risk Assessment:</b> ${escVal(parsed.risk_assessment)}`);
            codeAnalysisText = parts.join("<br><br>");
        } catch (e) {
            codeAnalysisText = esc(codeAnalysis);
            if (codeAnalysisText.length > 3000) {
                codeAnalysisText = codeAnalysisText.slice(0, 3000) + "... (truncated)";
            }
        }
    }

    if (engineerGuidance) {
        try {
            const parsed = JSON.parse(engineerGuidance);
            const escVal = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
            const parts = [];
            if (parsed.technical_assessment) parts.push(`<b>Technical Assessment:</b> ${escVal(parsed.technical_assessment)}`);
            if (parsed.verdict) parts.push(`<b>Verdict:</b> ${escVal(parsed.verdict)}`);
            if (parsed.effort_estimate) parts.push(`Effort Estimate: ${escVal(parsed.effort_estimate)}`);
            if (parsed.affected_files && parsed.affected_files.length > 0) {
                parts.push(`<b>Affected Files:</b><br>${parsed.affected_files.map(a => `&nbsp;&nbsp;• ${escVal(a)}`).join("<br>")}`);
            }
            if (parsed.implementation_approach) parts.push(`<b>Implementation Approach:</b> ${escVal(parsed.implementation_approach)}`);
            if (parsed.risks_and_tradeoffs) parts.push(`<b>Risks &amp; Tradeoffs:</b> ${escVal(parsed.risks_and_tradeoffs)}`);
            if (parsed.suggested_response) parts.push(`<b>Suggested Response to User:</b><br>${escVal(parsed.suggested_response)}`);
            if (parsed.related_considerations && parsed.related_considerations.length > 0) {
                parts.push(`<b>Related Considerations:</b><br>${parsed.related_considerations.map((s, i) => `&nbsp;&nbsp;${i + 1}. ${escVal(s)}`).join("<br>")}`);
            }
            engineerGuidanceText = parts.join("<br><br>");
        } catch (e) {
            engineerGuidanceText = esc(engineerGuidance);
            if (engineerGuidanceText.length > 3000) {
                engineerGuidanceText = engineerGuidanceText.slice(0, 3000) + "... (truncated)";
            }
        }
    }

    const htmlMessage = [
        `<h2>${emoji} mssql-python Issue Triage</h2>`,
        `<p><b>${esc(categoryDisplay)}</b> &nbsp;|&nbsp; `,
        `${sevIndicator} Severity: <b>${esc(severity)}</b> &nbsp;|&nbsp; `,
        `Confidence: <b>${analysis.confidence}%</b></p>`,
        `<hr>`,
        `<p>`,
        `📌 <b>Issue:</b> <a href="${issue.html_url}">#${issue.number} — ${esc(issue.title)}</a><br>`,
        `👤 <b>Author:</b> @${esc(issue.user.login)}<br>`,
        `🏷️ <b>Keywords:</b> ${esc(analysis.keywords.join(", "))}<br>`,
        `📂 <b>Relevant Files:</b> ${esc(analysis.relevant_source_files.join(", "))}`,
        `</p>`,
        `<hr>`,
        `<h3>📝 Analysis</h3>`,
        `<p>${esc(analysis.summary_for_maintainers)}</p>`,
        `<h3>🔍 Code Analysis</h3>`,
        `<p>${codeAnalysisText}</p>`,
        engineerGuidanceText ? `<h3>💡 Engineer Guidance</h3>` : '',
        engineerGuidanceText ? `<p>${engineerGuidanceText}</p>` : '',
        `<hr>`,
        `<p>⚡ <b>Action Required:</b> ${esc(action)}</p>`,
        `<p><i>⚠️ AI-generated analysis — verified against source code but may contain inaccuracies. Review before acting.</i></p>`,
        `<p><a href="${issue.html_url}">📋 View Issue</a>`,
        ` &nbsp;|&nbsp; `,
        `<a href="https://github.com/${REPO_OWNER}/${REPO_NAME}">📂 View Repository</a></p>`,
    ].join("");

    const payload = { text: htmlMessage };

    const response = await fetch(process.env.TEAMS_WEBHOOK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
    });

    if (!response.ok) {
        const text = await response.text();
        throw new Error(`Teams webhook error: ${response.status} - ${text}`);
    }

    return response.status;
}

// --- Main ---
async function main() {
    console.log(`\n🔍 Fetching issue #${issueNumber}...`);
    const issue = await fetchIssue(issueNumber);
    console.log(`   Title: ${issue.title}`);
    console.log(`   Author: ${issue.user.login}`);

    console.log(`\n🤖 Classifying with GitHub Models...`);
    const classificationPrompt = `
You are an expert triage system for the mssql-python repository — a Python driver for Microsoft SQL Server.
The driver uses ODBC under the hood with a C++/pybind11 native extension layer and Python wrappers.
Note: The pybind/ directory contains C++/pybind11 code (NOT Rust). Only reference Rust if the issue is specifically about BCP (Bulk Copy Protocol).

Key source files in the repo:
- mssql_python/connection.py — Connection management, pooling integration
- mssql_python/cursor.py — Cursor operations, execute, fetch, bulkcopy
- mssql_python/auth.py — Authentication (SQL auth, Azure AD, etc.)
- mssql_python/exceptions.py — Error handling and exception classes
- mssql_python/pooling.py — Connection pooling
- mssql_python/helpers.py — Utility functions
- mssql_python/constants.py — Constants, SQL types, enums
- mssql_python/connection_string_parser.py — Connection string parsing
- mssql_python/parameter_helper.py — Query parameter handling
- mssql_python/logging.py — Logging infrastructure
- mssql_python/row.py — Row objects
- mssql_python/type.py — Type mappings
- mssql_python/ddbc_bindings.py — Python/pybind11 ODBC bindings (C++ native extension, NOT Rust)
- mssql_python/pybind/ — C++/pybind11 native extension layer (NOT Rust)

Classify the following GitHub issue into EXACTLY ONE category:

1. FEATURE_REQUEST — User wants new functionality or enhancements
2. BUG — Something is broken, incorrect behavior, or errors
3. DISCUSSION — User is asking a question or wants clarification
4. BREAK_FIX — A regression or critical bug: segfaults, crashes, data corruption,
   or user says "this used to work"

Respond in this exact JSON format:
{
  "category": "BUG|FEATURE_REQUEST|DISCUSSION|BREAK_FIX",
  "confidence": <0-100>,
  "justification": "<2-3 sentence explanation>",
  "severity": "critical|high|medium|low",
  "relevant_source_files": ["<top 3 most relevant source file paths>"],
  "keywords": ["<key technical terms from the issue>"],
  "summary_for_maintainers": "<detailed 3-5 sentence analysis for maintainer notification>"
}

Issue Title: ${issue.title}
Issue Body:
${(issue.body || "").slice(0, 4000)}
`;

    const classifyResult = await callGitHubModels(classificationPrompt);
    const analysis = JSON.parse(classifyResult);

    console.log(`\n📊 Classification Results:`);
    console.log(`   Category:   ${analysis.category}`);
    console.log(`   Confidence: ${analysis.confidence}%`);
    console.log(`   Severity:   ${analysis.severity}`);
    console.log(`   Keywords:   ${analysis.keywords.join(", ")}`);
    console.log(`   Files:      ${analysis.relevant_source_files.join(", ")}`);
    console.log(`   Summary:    ${analysis.summary_for_maintainers}`);

    // --- Fetch relevant source files (for ALL categories) ---
    console.log(`\n📂 Fetching relevant source files for code-grounded analysis...`);
    const fileContents = [];
    for (const filePath of analysis.relevant_source_files.slice(0, 3)) {
        try {
            const content = await fetchFileContent(filePath);
            fileContents.push(`### File: ${filePath}\n\`\`\`python\n${content.slice(0, 3000)}\n\`\`\``);
            console.log(`   ✅ Fetched ${filePath}`);
        } catch (e) {
            console.log(`   ⚠️  Could not fetch ${filePath}: ${e.message}`);
        }
    }

    const codeContext = fileContents.length > 0
        ? `\n\nRelevant source files from the repository:\n${fileContents.join("\n\n")}`
        : '';

    // --- For BUG/BREAK_FIX, analyze codebase ---
    let codeAnalysis = "";

    if (["BUG", "BREAK_FIX"].includes(analysis.category) && fileContents.length > 0) {
        console.log(`\n🔬 Bug/Break-fix detected — analyzing codebase...`);

        const codePrompt = `
You are a senior Python developer analyzing a potential
${analysis.category === "BREAK_FIX" ? "regression/break-fix" : "bug"}
in the mssql-python driver (Python + ODBC + C++/pybind11 native layer).
IMPORTANT: ddbc_bindings.py and the pybind/ directory are C++/pybind11 code, NOT Rust. Only mention Rust if the issue is specifically about BCP (Bulk Copy Protocol).
IMPORTANT: Base your analysis ONLY on the actual source code provided below. Do not speculate about code you haven't seen.

Bug Report:
Title: ${issue.title}
Body: ${(issue.body || "").slice(0, 2000)}
${codeContext}

Provide analysis in JSON:
{
  "is_bug": "Confirmed Bug|Likely Bug|Require More Analysis|Not a Bug",
  "root_cause": "<detailed root cause analysis based on actual code above>",
  "affected_components": ["<affected modules/functions from the code above>"],
  "evidence_and_context": "<specific evidence from the codebase — cite exact functions, variables, line logic, or patterns that support your analysis>",
  "recommended_fixes": ["<fix 1 — describe the approach referencing specific code>", "<fix 2>", "<fix 3>"],
  "code_locations": ["<file:function or file:class where changes should be made>"],
  "risk_assessment": "<risk to users>"
}
`;

        try {
            codeAnalysis = await callGitHubModels(codePrompt);
            const parsed = JSON.parse(codeAnalysis);
            console.log(`\n🔍 Code Analysis:`);
            console.log(`   Is Bug:     ${parsed.is_bug}`);
            console.log(`   Root Cause: ${parsed.root_cause}`);
            if (parsed.evidence_and_context) {
                console.log(`   Evidence:   ${parsed.evidence_and_context}`);
            }
            if (parsed.recommended_fixes && parsed.recommended_fixes.length > 0) {
                console.log(`\n\ud83d\udee0\ufe0f  Recommended Fixes:`);
                for (const fix of parsed.recommended_fixes) {
                    console.log(`   \u2022 ${fix}`);
                }
            }
            if (parsed.code_locations && parsed.code_locations.length > 0) {
                console.log(`\n\ud83d\udccd Code Locations:`);
                for (const loc of parsed.code_locations) {
                    console.log(`   \u2022 ${loc}`);
                }
            }
        } catch (e) {
            console.log(`   ⚠️  Code analysis failed: ${e.message}`);
        }
    }

    // --- For FEATURE_REQUEST/DISCUSSION, provide code-grounded engineer guidance ---
    let engineerGuidance = "";

    if (["FEATURE_REQUEST", "DISCUSSION"].includes(analysis.category)) {
        console.log(`\n💡 Non-bug issue — generating code-grounded engineer guidance...`);

        const guidancePrompt = `
You are a senior engineer on the mssql-python team — a Python driver for Microsoft SQL Server
(ODBC + C++/pybind11 native extension + Python wrappers).
IMPORTANT: Base your analysis ONLY on the actual source code provided below. Do not speculate about code you haven't seen. If the code doesn't contain enough information, say so explicitly.

A user filed a GitHub issue classified as: ${analysis.category}

Issue Title: ${issue.title}
Issue Body:
${(issue.body || "").slice(0, 3000)}
${codeContext}

Based on the ACTUAL SOURCE CODE above, provide a detailed analysis to help the engineering team respond efficiently.
Respond in JSON:
{
  "technical_assessment": "<detailed technical assessment grounded in the actual code above>",
  "verdict": "Confirmed Bug|Likely Bug|Require More Analysis|Not a Bug",
  "issue_identified": true/false,
  "affected_files": ["<specific source files, modules, functions, or classes from the code above>"],
  "current_behavior": "<describe what the current code actually does based on your reading>",
  "implementation_approach": "<concrete implementation steps referencing specific functions/lines from the code — ONLY if issue_identified is true, otherwise empty string>",
  "effort_estimate": "small|medium|large|epic",
  "risks_and_tradeoffs": "<potential risks, backward compatibility concerns, or tradeoffs — ONLY if issue_identified is true, otherwise empty string>",
  "suggested_response": "<a draft response the engineer could post on the issue. Always ask the user to share a minimal repro or code snippet that demonstrates the issue or desired behavior, if they haven't already provided one.>",
  "related_considerations": ["<other things the team should think about — ONLY if issue_identified is true, otherwise empty array>"]
}

IMPORTANT: If your technical_assessment does not identify any actual issue or gap in the code, set issue_identified to false and leave implementation_approach, risks_and_tradeoffs, and related_considerations empty. Only populate those fields when a real problem or improvement opportunity is confirmed in the code.
`;

        try {
            engineerGuidance = await callGitHubModels(guidancePrompt);
            const parsed = JSON.parse(engineerGuidance);
            console.log(`\n💡 Engineer Guidance:`);
            console.log(`   Verdict:         ${parsed.verdict}`);
            console.log(`   Effort:          ${parsed.effort_estimate}`);
            console.log(`   Current Code:    ${parsed.current_behavior}`);
            console.log(`   Assessment:      ${parsed.technical_assessment}`);
            console.log(`   Approach:        ${parsed.implementation_approach}`);
            console.log(`   Risks:           ${parsed.risks_and_tradeoffs}`);
        } catch (e) {
            console.log(`   ⚠️  Engineer guidance failed: ${e.message}`);
        }
    }

    // --- Send Teams notification ---
    console.log(`\n📤 Sending Teams notification...`);
    try {
        const status = await sendTeamsNotification(analysis, codeAnalysis, engineerGuidance, issue);
        console.log(`   ✅ Teams notification sent (HTTP ${status})`);
    } catch (e) {
        console.error(`   ❌ Teams notification failed: ${e.message}`);
    }

    console.log(`\n✅ Triage complete for issue #${issueNumber}`);
}

main().catch((e) => {
    console.error(`\n❌ Fatal error: ${e.message}`);
    process.exit(1);
});
