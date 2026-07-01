defmodule Interruptus.Policy.Restart do
  @moduledoc """
  Restart policy: attempt counting, backoff scheduling, and retryable error filtering.

  Policies are normalized maps produced by `restart_policy/0` on workflow modules:

      %{
        max_attempts: 3,
        backoff: :exponential,
        base_interval: 1_000,
        retryable_errors: :all
      }

  Configure via the `restart_policy/1` macro in `Interruptus.Workflow`.
  """

  @doc """
  Returns whether another attempt should be made.

  ## Arguments

    * `policy` - restart policy map with `max_attempts`
    * `attempt_count` - number of attempts already recorded on the instance

  ## Returns

    * `true` when `attempt_count < policy.max_attempts`
    * `false` otherwise

  ## Examples

      iex> policy = %{max_attempts: 3, backoff: :exponential, base_interval: 1000, retryable_errors: :all}
      iex> Interruptus.Policy.Restart.retry?(policy, 2)
      true
      iex> Interruptus.Policy.Restart.retry?(policy, 3)
      false
  """
  @spec retry?(map(), non_neg_integer()) :: boolean()
  def retry?(policy, attempt_count) do
    attempt_count < policy.max_attempts
  end

  @doc """
  Computes backoff delay in milliseconds for the given attempt.

  For `:constant` backoff, returns `base_interval` every time.
  For `:exponential` backoff, returns `base_interval * 2^(attempt - 1)`.

  ## Arguments

    * `policy` - restart policy map with `backoff` and `base_interval`
    * `attempt` - 1-based attempt number for the upcoming retry

  ## Returns

    * Non-negative delay in milliseconds

  ## Examples

      iex> policy = %{max_attempts: 3, backoff: :constant, base_interval: 500, retryable_errors: :all}
      iex> Interruptus.Policy.Restart.backoff_ms(policy, 2)
      500

      iex> policy = %{max_attempts: 3, backoff: :exponential, base_interval: 1000, retryable_errors: :all}
      iex> Interruptus.Policy.Restart.backoff_ms(policy, 2)
      2000
  """
  @spec backoff_ms(map(), pos_integer()) :: non_neg_integer()
  def backoff_ms(%{backoff: :constant, base_interval: base}, _attempt), do: base

  def backoff_ms(%{backoff: :exponential, base_interval: base}, attempt) do
    trunc(base * :math.pow(2, attempt - 1))
  end

  @doc """
  Returns whether the error is retryable per policy.

  When `retryable_errors` is `:all`, every error is retryable. When it is a list,
  only listed error terms are retryable.

  ## Arguments

    * `policy` - restart policy map with `retryable_errors`
    * `reason` - error term from stage or verify failure

  ## Returns

    * `true` when the error should trigger a retry
    * `false` otherwise

  ## Examples

      iex> policy = %{max_attempts: 3, backoff: :exponential, base_interval: 1000, retryable_errors: :all}
      iex> Interruptus.Policy.Restart.retryable?(policy, :timeout)
      true

      iex> policy = %{max_attempts: 3, backoff: :exponential, base_interval: 1000, retryable_errors: [:timeout]}
      iex> Interruptus.Policy.Restart.retryable?(policy, :timeout)
      true
      iex> Interruptus.Policy.Restart.retryable?(policy, :halted)
      false
  """
  @spec retryable?(map(), term()) :: boolean()
  def retryable?(%{retryable_errors: :all}, _reason), do: true

  def retryable?(%{retryable_errors: errors}, reason) when is_list(errors) do
    reason in errors
  end
end
