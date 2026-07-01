defmodule Interruptus.Policy.Restart do
  @moduledoc """
  Restart policy: attempt counting and backoff scheduling.
  """

  @doc """
  Returns whether another attempt should be made.
  """
  @spec retry?(map(), non_neg_integer()) :: boolean()
  def retry?(policy, attempt_count) do
    attempt_count < policy.max_attempts
  end

  @doc """
  Computes backoff delay in milliseconds for the given attempt.
  """
  @spec backoff_ms(map(), pos_integer()) :: non_neg_integer()
  def backoff_ms(%{backoff: :constant, base_interval: base}, _attempt), do: base

  def backoff_ms(%{backoff: :exponential, base_interval: base}, attempt) do
    trunc(base * :math.pow(2, attempt - 1))
  end

  @doc """
  Returns whether the error is retryable per policy.
  """
  @spec retryable?(map(), term()) :: boolean()
  def retryable?(%{retryable_errors: :all}, _reason), do: true

  def retryable?(%{retryable_errors: errors}, reason) when is_list(errors) do
    reason in errors
  end
end
