import type { ErrorInfo, PropsWithChildren, ReactNode } from "react";
import { Component } from "react";

type ErrorBoundaryFallback = (props: { error: unknown; reset: () => void }) => ReactNode;

type ErrorBoundaryProps = PropsWithChildren<{
  fallback: ErrorBoundaryFallback;
}>;

type ErrorBoundaryState = {
  error: unknown;
};

export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  override state: ErrorBoundaryState = {
    error: null,
  };

  static getDerivedStateFromError(error: unknown): ErrorBoundaryState {
    return { error };
  }

  override componentDidCatch(error: unknown, errorInfo: ErrorInfo) {
    console.error(error, errorInfo);
  }

  reset = () => {
    this.setState({ error: null });
  };

  override render() {
    if (this.state.error) {
      return this.props.fallback({
        error: this.state.error,
        reset: this.reset,
      });
    }

    return this.props.children;
  }
}
