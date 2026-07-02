# Credo configuration for Interruptus.
#
# Philosophy: catch real readability problems and likely bugs; leave formatting
# and stylistic preferences to `.formatter.exs` and team convention.
#
# Run:  mix credo
# Strict (includes low-priority checks): mix credo --strict

%{
  configs: [
    %{
      name: "default",
      files: %{
        included: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
        excluded: [~r"/_build/", ~r"/deps/", ~r"/node_modules/"]
      },
      plugins: [],
      requires: [],
      strict: false,
      parse_timeout: 5000,
      color: true,
      checks: %{
        disabled: [
          # --- Consistency (formatter territory) ---
          {Credo.Check.Consistency.LineEndings, []},
          {Credo.Check.Consistency.SpaceAroundOperators, []},
          {Credo.Check.Consistency.SpaceInParentheses, []},
          {Credo.Check.Consistency.TabsOrSpaces, []},

          # --- Design (noise vs. signal) ---
          {Credo.Check.Design.AliasUsage, []},

          # --- Readability (style nitpicks) ---
          {Credo.Check.Readability.AliasOrder, []},
          {Credo.Check.Readability.MaxLineLength, []},
          {Credo.Check.Readability.ParenthesesOnZeroArityDefs, []},
          {Credo.Check.Readability.PipeIntoAnonymousFunctions, []},
          {Credo.Check.Readability.PreferImplicitTry, []},
          {Credo.Check.Readability.RedundantBlankLines, []},
          {Credo.Check.Readability.Semicolons, []},
          {Credo.Check.Readability.SpaceAfterCommas, []},
          {Credo.Check.Readability.TrailingBlankLine, []},
          {Credo.Check.Readability.TrailingWhiteSpace, []},
          {Credo.Check.Readability.UnnecessaryAliasExpansion, []},
          {Credo.Check.Readability.WithSingleClause, []},

          # --- Refactor (micro-optimizations / subjective rewrites) ---
          {Credo.Check.Refactor.Apply, []},
          {Credo.Check.Refactor.FilterCount, []},
          {Credo.Check.Refactor.FilterFilter, []},
          {Credo.Check.Refactor.MapJoin, []},
          {Credo.Check.Refactor.NegatedConditionsInUnless, []},
          {Credo.Check.Refactor.NegatedConditionsWithElse, []},
          {Credo.Check.Refactor.RejectReject, []},
          {Credo.Check.Refactor.RedundantWithClauseResult, []}
        ],
        extra: [
          # TODO comments are useful markers; don't fail CI on them.
          {Credo.Check.Design.TagTODO, [exit_status: 0]},

          # Complexity thresholds tuned for clarity without being punitive.
          {Credo.Check.Refactor.CyclomaticComplexity, [max_complexity: 12]},
          {Credo.Check.Refactor.Nesting, [max_nesting: 3]},
          {Credo.Check.Refactor.FunctionArity, [max_arity: 8]}
        ]
      }
    },
    %{
      name: "test",
      files: %{
        included: ["test/**/*.{ex,exs}"],
        excluded: [~r"/_build/", ~r"/deps/"]
      },
      strict: false,
      checks: %{
        disabled: [
          # Test modules are support code; don't require @moduledoc on every file.
          {Credo.Check.Readability.ModuleDoc, []}
        ]
      }
    }
  ]
}
