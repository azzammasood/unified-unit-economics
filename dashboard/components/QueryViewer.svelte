<script>
  // Evidence injects QueryViewer for SQL blocks in some versions.
  // This minimal component keeps builds working even when the
  // default component plugin isn't available.
  export let query;
  export let title;
  export let open = false;
</script>

{#if open}
  <details open>
    <summary>{title ?? "Query"}</summary>
    <pre style="white-space: pre-wrap; overflow-x: auto; padding: 0.75rem; border: 1px solid rgba(0,0,0,0.15); border-radius: 0.5rem;">
{query}
    </pre>
  </details>
{:else}
  <details>
    <summary>{title ?? "Query"}</summary>
    <pre style="white-space: pre-wrap; overflow-x: auto; padding: 0.75rem; border: 1px solid rgba(0,0,0,0.15); border-radius: 0.5rem;">
{query}
    </pre>
  </details>
{/if}

