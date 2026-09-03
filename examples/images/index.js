export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/info") {
      const info = await env.IMAGES.input(request).info();
      return Response.json(info);
    }
    if (url.pathname === "/resize") {
      const width = Number(url.searchParams.get("w") || "64");
      const height = Number(url.searchParams.get("h") || "64");
      return env.IMAGES
        .input(request)
        .transform({ width, height, fit: "cover" })
        .output({ format: "image/png" })
        .response();
    }
    return new Response("POST an image to /resize or /info\n", { status: 200 });
  },
};
